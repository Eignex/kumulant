@file:OptIn(ExperimentalAtomicApi::class)

package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasObservationCount
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.Mutex
import com.eignex.kumulant.stream.NoopMutex
import com.eignex.kumulant.stream.PlatformMutex
import com.eignex.kumulant.stream.guarded
import kotlin.concurrent.Volatile
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.random.Random

// Shared VFDT engine over distinct public regression and classification node hierarchies.

// Narrowing members return null for nodes of another kind.
internal interface TreeShape<Row, S : Split<Row>, N : Any, SN : N, AL : N, P : HasObservationCount> {

    fun asSplitNode(node: N): SN?

    fun asAuditLeaf(node: N): AL?

    fun leafArm(node: N): SeriesStat<P>?

    fun splitOf(node: SN): S

    fun posOf(node: SN): N

    fun negOf(node: SN): N

    fun setPos(node: SN, child: N)

    fun setNeg(node: SN, child: N)

    fun carryoverOf(node: SN): SeriesStat<P>?

    fun setCarryover(node: SN, arm: SeriesStat<P>)

    fun candidatesOf(leaf: AL): List<S>

    fun posArmsOf(leaf: AL): List<SeriesStat<P>>

    fun negArmsOf(leaf: AL): List<SeriesStat<P>>

    fun ticksOf(leaf: AL): AtomicLong

    fun makeSplitNode(split: S, pos: N, neg: N, carryover: SeriesStat<P>?): SN

    fun makeTerminalLeaf(arm: SeriesStat<P>): N

    fun makeAuditLeaf(arm: SeriesStat<P>, candidates: List<S>, pos: List<SeriesStat<P>>, neg: List<SeriesStat<P>>): N

    fun emptyArm(): SeriesStat<P>

    fun mergePayload(a: P, b: P): P

    fun subtractPayload(total: P, part: P): P

    fun aggregate(node: N): P

    fun rank(total: P, pos: List<P>, neg: List<P>): SplitInfo

    fun leafLabel(arm: P): String
}

// Snapshot shapes are immutable and serializable over dense vector rows; live shapes support arbitrary rows.
internal interface TreeResultShape<S, R : Any, RS : R, P : HasObservationCount> {

    fun asSplitResult(node: R): RS?

    fun splitOf(node: RS): S

    fun posOf(node: RS): R

    fun negOf(node: RS): R

    fun valueOf(node: R): P
}

// Leaf updates follow the arm concurrency contract; structural mutations share a per-tree lock.
internal class TreeGrowth<Row, S : Split<Row>, N : Any, SN : N, AL : N, P : HasObservationCount>(
    private val shape: TreeShape<Row, S, N, SN, AL, P>,
    private val splitCandidates: List<S>,
    private val config: HoeffdingTreeConfig,
    concurrency: Concurrency,
    randomSeed: Int,
) {
    private val random = Random(randomSeed)
    private val canGrow: Boolean = splitCandidates.isNotEmpty()

    /** Serialises split conversions and structural merges. */
    val splitLock: Mutex = if (concurrency == Concurrency.None) NoopMutex else PlatformMutex()

    /** Internal plus leaf nodes currently in the tree. */
    val nbrNodes: AtomicInt = AtomicInt(1)

    /** Live root. Volatile because the hot path reads it without the lock. */
    @Volatile
    var root: N = newLeaf(depth = 0)

    /** Fold an observation into the tree, possibly growing it. */
    fun update(row: Row, value: Double, weight: Double) {
        val current = root
        val next = updateNode(current, row, value, weight, depth = 0)
        if (next !== current) root = next
    }

    /** Reset to a single fresh leaf. */
    fun reset() {
        splitLock.guarded {
            nbrNodes.store(1)
            root = newLeaf(depth = 0)
        }
    }

    /** Render the tree as nested `if (split) { ... } else { ... }` text. */
    fun prettyPrint(indent: String): String = buildString { printTo(this, root, indent) }

    /**
     * Structurally merge another tree's root into this one; the other side is consumed.
     *
     *  - **Same split predicate**: recurse on both children; internal aggregates are derived from leaves,
     *    so no per-split merge step is needed.
     *  - **Both leaves**: merge arms directly.
     *  - **Self leaf, other split**: adopt the other structure wholesale and fold this leaf's aggregate
     *    into the adopted subtree's leftmost leaf.
     *  - **Self split, other leaf**, or **different splits**: keep this structure and fold the other
     *    aggregate, recursively combined if it is a split, into this tree's leftmost leaf.
     *
     * The leftmost-leaf rule preserves the merged total weight but biases the un-routable observations
     * into a single bucket; an honest fallback when the structures do not align.
     */
    fun mergeTree(other: N) {
        splitLock.guarded {
            root = mergeNodes(root, other)
            nbrNodes.store(countNodes(root))
        }
    }

    /** Snapshot merge: the same rules as [mergeTree], but the other side is an immutable snapshot. */
    fun <R : Any, RS : R> mergeSnapshot(other: R, results: TreeResultShape<S, R, RS, P>) {
        splitLock.guarded {
            root = mergeNodeWithResult(root, other, results, depth = 0)
            nbrNodes.store(countNodes(root))
        }
    }

    // Each sibling checks the node ceiling at birth.
    private fun newLeaf(depth: Int): N {
        if (depth >= config.maxDepth || nbrNodes.load() + 1 > config.maxNodes || !canGrow) {
            return shape.makeTerminalLeaf(shape.emptyArm())
        }
        val subset = splitCandidates.pickCandidates(config.mtry, random)
        return shape.makeAuditLeaf(
            arm = shape.emptyArm(),
            candidates = subset,
            pos = List(subset.size) { shape.emptyArm() },
            neg = List(subset.size) { shape.emptyArm() },
        )
    }

    /** Internal plus leaf nodes in the subtree at [node]. */
    fun countNodes(node: N): Int {
        val split = shape.asSplitNode(node) ?: return 1
        return 1 + countNodes(shape.posOf(split)) + countNodes(shape.negOf(split))
    }

    private fun updateNode(node: N, row: Row, value: Double, weight: Double, depth: Int): N {
        val split = shape.asSplitNode(node)
        if (split != null) {
            if (shape.splitOf(split).direction(row)) {
                val child = shape.posOf(split)
                val next = updateNode(child, row, value, weight, depth + 1)
                if (next !== child) shape.setPos(split, next)
            } else {
                val child = shape.negOf(split)
                val next = updateNode(child, row, value, weight, depth + 1)
                if (next !== child) shape.setNeg(split, next)
            }
            return node
        }
        val arm = shape.leafArm(node) ?: return node
        arm.update(value, 0L, weight)
        val audit = shape.asAuditLeaf(node) ?: return node
        return updateAuditLeaf(node, audit, arm, row, value, weight, depth)
    }

    // updateNode has already updated the leaf arm; this path updates candidate arms and audits splits.
    private fun updateAuditLeaf(
        node: N,
        leaf: AL,
        arm: SeriesStat<P>,
        row: Row,
        value: Double,
        weight: Double,
        depth: Int,
    ): N {
        val candidates = shape.candidatesOf(leaf)
        val posArms = shape.posArmsOf(leaf)
        val negArms = shape.negArmsOf(leaf)
        for ((i, split) in candidates.withIndex()) {
            if (split.direction(row)) {
                posArms[i].update(value, 0L, weight)
            } else {
                negArms[i].update(value, 0L, weight)
            }
        }
        val ticks = shape.ticksOf(leaf)
        if (ticks.addAndFetch(1L) < config.splitPeriod) return node

        return splitLock.guarded {
            // Double-check inside the lock: another thread may have already audited
            // (and reset the counter) or replaced this leaf.
            if (ticks.load() < config.splitPeriod) return@guarded node
            ticks.store(0L)

            // Split evidence comes from locally routed candidate observations; merged arm mass lacks candidate data.
            if (candidates.isEmpty()) return@guarded node
            val posResults = posArms.map { it.read(0L) }
            val negResults = negArms.map { it.read(0L) }
            val attributed = shape.mergePayload(posResults[0], negResults[0])
            val ranked = shape.rank(attributed, posResults, negResults)
            // shouldSplit holds every gate: the weight floor, a scorable winner, and the
            // Hoeffding-versus-tau disjunction.
            if (!shouldSplit(ranked, attributed.totalWeights, depth, config)) return@guarded node

            // Stash the pre-split aggregate as the new split's carryover so it shows up in subtree
            // aggregates without burdening the hot path. New leaves start empty, so prior-driven
            // Thompson/UCB exploration is not skewed by data the split already accounted for.
            nbrNodes.addAndFetch(2)
            shape.makeSplitNode(
                split = candidates[ranked.bestIndex],
                pos = newLeaf(depth + 1),
                neg = newLeaf(depth + 1),
                carryover = arm,
            )
        }
    }

    private fun mergeNodes(a: N, b: N): N {
        val aArm = shape.leafArm(a)
        if (aArm != null) {
            val bArm = shape.leafArm(b)
            if (bArm != null) {
                aArm.merge(bArm.read(0L))
                return a
            }
            val bSplit = shape.asSplitNode(b) ?: return a
            foldIntoCarryover(bSplit, aArm.read(0L))
            return b
        }
        val aSplit = shape.asSplitNode(a) ?: return a
        val bSplit = shape.asSplitNode(b)
        if (bSplit != null && shape.splitOf(aSplit) == shape.splitOf(bSplit)) {
            shape.setPos(aSplit, mergeNodes(shape.posOf(aSplit), shape.posOf(bSplit)))
            shape.setNeg(aSplit, mergeNodes(shape.negOf(aSplit), shape.negOf(bSplit)))
            val bCarry = shape.carryoverOf(bSplit)
            if (bCarry != null) foldIntoCarryover(aSplit, bCarry.read(0L))
            return a
        }
        // a split + b leaf, or the splits differ: keep a's structure, fold b's aggregate.
        foldIntoCarryover(aSplit, shape.aggregate(b))
        return a
    }

    private fun <R : Any, RS : R> mergeNodeWithResult(
        a: N,
        b: R,
        results: TreeResultShape<S, R, RS, P>,
        depth: Int,
    ): N {
        val bSplit = results.asSplitResult(b)
        val aArm = shape.leafArm(a)
        if (aArm != null) {
            if (bSplit == null) {
                aArm.merge(results.valueOf(b))
                return a
            }
            val cloned = cloneFromResult(b, results, depth)
            val clonedSplit = shape.asSplitNode(cloned) ?: return cloned
            foldIntoCarryover(clonedSplit, aArm.read(0L))
            return cloned
        }
        val aSplit = shape.asSplitNode(a) ?: return a
        if (bSplit != null && shape.splitOf(aSplit) == results.splitOf(bSplit)) {
            shape.setPos(
                aSplit,
                mergeNodeWithResult(shape.posOf(aSplit), results.posOf(bSplit), results, depth + 1),
            )
            shape.setNeg(
                aSplit,
                mergeNodeWithResult(shape.negOf(aSplit), results.negOf(bSplit), results, depth + 1),
            )
            // b's value carries the full subtree aggregate; the child recursion already folded the
            // structurally-aligned portion. Pull only the residual, what b's value holds beyond the sum
            // of its children, into a's carryover.
            val residual = residualOf(bSplit, results)
            if (residual.totalWeights > 0.0) foldIntoCarryover(aSplit, residual)
            return a
        }
        // a split + b leaf, or the splits differ: keep a's structure, fold b's aggregate.
        foldIntoCarryover(aSplit, results.valueOf(b))
        return a
    }

    // Cloning preserves snapshot shape even when its depth exceeds this tree configuration.
    private fun <R : Any, RS : R> cloneFromResult(node: R, results: TreeResultShape<S, R, RS, P>, depth: Int): N {
        nbrNodes.addAndFetch(1)
        val split = results.asSplitResult(node)
        if (split == null) {
            // newLeaf, not makeTerminalLeaf: a cloned leaf that cannot audit would freeze the merged
            // tree at the shape of whatever snapshot it adopted.
            val leaf = newLeaf(depth)
            shape.leafArm(leaf)?.merge(results.valueOf(node))
            return leaf
        }
        val pos = cloneFromResult(results.posOf(split), results, depth + 1)
        val neg = cloneFromResult(results.negOf(split), results, depth + 1)
        // Re-establish any orphan aggregate the snapshot encodes by comparing the recorded
        // value against the sum of the cloned children's aggregates.
        val residual = residualOf(split, results)
        val carry = if (residual.totalWeights > 0.0) shape.emptyArm().also { it.merge(residual) } else null
        return shape.makeSplitNode(results.splitOf(split), pos, neg, carry)
    }

    // Carryover is the split aggregate minus its child aggregates.
    private fun <R : Any, RS : R> residualOf(split: RS, results: TreeResultShape<S, R, RS, P>): P {
        val childSum = shape.mergePayload(
            results.valueOf(results.posOf(split)),
            results.valueOf(results.negOf(split)),
        )
        return shape.subtractPayload(results.valueOf(split), childSum)
    }

    private fun foldIntoCarryover(node: SN, value: P) {
        if (value.totalWeights <= 0.0) return
        val existing = shape.carryoverOf(node)
        if (existing != null) {
            existing.merge(value)
        } else {
            shape.setCarryover(node, shape.emptyArm().also { it.merge(value) })
        }
    }

    private fun printTo(sb: StringBuilder, node: N, indent: String) {
        val split = shape.asSplitNode(node)
        if (split == null) {
            val arm = shape.leafArm(node) ?: return
            sb.append(indent).append(shape.leafLabel(arm.read(0L))).append('\n')
            return
        }
        sb.append(indent).append("if (").append(shape.splitOf(split).toString()).append(") {\n")
        printTo(sb, shape.posOf(split), "$indent  ")
        sb.append(indent).append("} else {\n")
        printTo(sb, shape.negOf(split), "$indent  ")
        sb.append(indent).append("}\n")
    }
}
