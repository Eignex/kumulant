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

/*
 * The VFDT growth engine, shared by [RegressionTree] and [ClassificationTree].
 *
 * The trees run one algorithm over two payload types, but their node hierarchies are public and
 * wire-visible, so the types must stay distinct: `RegressionNode` is generic in the row type and carries
 * weighted-variance arms, `ClassificationNode` is fixed to a dense vector row and carries class-count
 * arms. What is shared is therefore the engine, parameterised by [TreeShape] - an internal SPI that
 * narrows nodes, reads and writes their children, and builds new ones.
 */

/**
 * The node-shape SPI the growth engine drives. Every member is a one-liner over one tree's node types.
 *
 * The narrowing members ([asSplitNode], [asAuditLeaf], [leafArm]) return null when the node is not of
 * that kind; the engine dispatches on a chain of null checks rather than a `when` over a sealed type.
 */
internal interface TreeShape<Row, S : Split<Row>, N : Any, SN : N, AL : N, P : HasObservationCount> {

    /** The node as a split node, or null when it is a leaf. */
    fun asSplitNode(node: N): SN?

    /** The node as an audit leaf, or null when it is a split or a terminal leaf. */
    fun asAuditLeaf(node: N): AL?

    /** The leaf's live accumulator, or null when the node is a split (splits hold no live arm). */
    fun leafArm(node: N): SeriesStat<P>?

    /** The split node's routing predicate. */
    fun splitOf(node: SN): S

    /** The child taken when the predicate holds. */
    fun posOf(node: SN): N

    /** The child taken when the predicate does not hold. */
    fun negOf(node: SN): N

    /** Repoint the true-side child. */
    fun setPos(node: SN, child: N)

    /** Repoint the false-side child. */
    fun setNeg(node: SN, child: N)

    /** The split's orphan-aggregate arm, or null when it holds none. */
    fun carryoverOf(node: SN): SeriesStat<P>?

    /** Install an orphan-aggregate arm on the split. */
    fun setCarryover(node: SN, arm: SeriesStat<P>)

    /** The candidate splits this audit leaf is evaluating. */
    fun candidatesOf(leaf: AL): List<S>

    /** Per-candidate accumulators for observations routing true. */
    fun posArmsOf(leaf: AL): List<SeriesStat<P>>

    /** Per-candidate accumulators for observations routing false. */
    fun negArmsOf(leaf: AL): List<SeriesStat<P>>

    /** The audit leaf's split-period tick counter. */
    fun ticksOf(leaf: AL): AtomicLong

    /** Build a split node. */
    fun makeSplitNode(split: S, pos: N, neg: N, carryover: SeriesStat<P>?): SN

    /** Build a leaf that will never be audited again. */
    fun makeTerminalLeaf(arm: SeriesStat<P>): N

    /** Build a leaf that audits the given candidate subset. */
    fun makeAuditLeaf(arm: SeriesStat<P>, candidates: List<S>, pos: List<SeriesStat<P>>, neg: List<SeriesStat<P>>): N

    /** A fresh, empty accumulator of the tree's payload type. */
    fun emptyArm(): SeriesStat<P>

    /** Combine two payload aggregates. */
    fun mergePayload(a: P, b: P): P

    /** Recover the other summand of a merge; the inverse of [mergePayload]. */
    fun subtractPayload(total: P, part: P): P

    /** Aggregate over every observation routed through the subtree at this node. */
    fun aggregate(node: N): P

    /** Score the candidates at a leaf under the tree's configured split criterion. */
    fun rank(total: P, pos: List<P>, neg: List<P>): SplitInfo

    /** The `prettyPrint` text for a leaf holding this aggregate, without indent or newline. */
    fun leafLabel(arm: P): String
}

/**
 * The snapshot-shape SPI, used only by the snapshot-merge paths.
 *
 * Kept separate from [TreeShape] because the snapshot hierarchies are immutable, serializable, and only
 * ever seen over a dense vector row, while [TreeShape] describes live nodes over an arbitrary row type.
 */
internal interface TreeResultShape<S, R : Any, RS : R, P : HasObservationCount> {

    /** The snapshot node as a split snapshot, or null when it is a leaf snapshot. */
    fun asSplitResult(node: R): RS?

    /** The split snapshot's routing predicate. */
    fun splitOf(node: RS): S

    /** The subtree snapshot taken when the predicate holds. */
    fun posOf(node: RS): R

    /** The subtree snapshot taken when the predicate does not hold. */
    fun negOf(node: RS): R

    /** The aggregate this snapshot node records for its whole subtree. */
    fun valueOf(node: R): P
}

/**
 * Owns one tree: its root pointer, node count, split lock, PRNG and candidate pool, and every operation
 * that reads or rewrites its structure.
 *
 * Concurrency: leaf-arm updates run lock-free and honour whatever contract the arms were built with; the
 * split-conversion path, the only one that mutates structure, is serialised by a single per-tree lock and
 * fires only every `splitPeriod` observations per audit leaf. Pointer writes on the hot path are skipped
 * when the child reference is unchanged.
 */
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
    fun <R : Any, RS : R> mergeSnapshot(
        other: R,
        results: TreeResultShape<S, R, RS, P>,
        workspace: com.eignex.koblas.Workspace? = null,
    ) {
        splitLock.guarded {
            root = mergeNodeWithResult(root, other, results, depth = 0, workspace)
            nbrNodes.store(countNodes(root))
        }
    }

    /**
     * A newborn leaf at [depth]: an audit leaf if the tree may still grow there, a terminal leaf if not.
     *
     * The node ceiling is tested as `nbrNodes.load() + 1` rather than the two nodes a split actually adds,
     * because it is checked at each leaf birth and the sibling's own birth checks it again.
     */
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

    /**
     * Route one observation to its leaf and fold it in, returning the node that should sit in this slot
     * afterwards - the same node, unless an audit leaf converted itself into a split.
     */
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

    /**
     * Fold the observation into every candidate's pos/neg sub-arm, then, on the observations that land on
     * a split-period boundary, decide whether the leaf has earned a split.
     *
     * The leaf's own arm was already updated by [updateNode], which is shared with the terminal-leaf path.
     */
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

            // Every candidate partitions the same locally routed observations, so either side of the
            // first one sums back to exactly what this leaf can attribute. That, not the leaf arm, is
            // the evidence a split decision rests on: a merge folds mass into the arm that no
            // candidate ever saw, and a larger n both shrinks the Hoeffding bound and clears the
            // weight floor on data the comparison never examined.
            // An audit leaf is only ever born with a non-empty candidate subset (canGrow gates that),
            // but mtry is caller data, so index nothing without checking.
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
        workspace: com.eignex.koblas.Workspace?,
    ): N {
        val bSplit = results.asSplitResult(b)
        val aArm = shape.leafArm(a)
        if (aArm != null) {
            if (bSplit == null) {
                aArm.merge(results.valueOf(b), workspace)
                return a
            }
            val cloned = cloneFromResult(b, results, depth, workspace)
            val clonedSplit = shape.asSplitNode(cloned) ?: return cloned
            foldIntoCarryover(clonedSplit, aArm.read(0L), workspace)
            return cloned
        }
        val aSplit = shape.asSplitNode(a) ?: return a
        if (bSplit != null && shape.splitOf(aSplit) == results.splitOf(bSplit)) {
            shape.setPos(
                aSplit,
                mergeNodeWithResult(shape.posOf(aSplit), results.posOf(bSplit), results, depth + 1, workspace),
            )
            shape.setNeg(
                aSplit,
                mergeNodeWithResult(shape.negOf(aSplit), results.negOf(bSplit), results, depth + 1, workspace),
            )
            // b's value carries the full subtree aggregate; the child recursion already folded the
            // structurally-aligned portion. Pull only the residual, what b's value holds beyond the sum
            // of its children, into a's carryover.
            val residual = residualOf(bSplit, results)
            if (residual.totalWeights > 0.0) foldIntoCarryover(aSplit, residual, workspace)
            return a
        }
        // a split + b leaf, or the splits differ: keep a's structure, fold b's aggregate.
        foldIntoCarryover(aSplit, results.valueOf(b), workspace)
        return a
    }

    /**
     * Rebuild a live subtree from a snapshot.
     *
     * Deliberately unbounded by depth: [newLeaf] refuses to grow past `maxDepth`, but cloning honours the
     * snapshot's shape whatever it is. A snapshot normally comes from a tree that already respected its own
     * `maxDepth`, so the two only diverge when the two configs differ.
     */
    private fun <R : Any, RS : R> cloneFromResult(
        node: R,
        results: TreeResultShape<S, R, RS, P>,
        depth: Int,
        workspace: com.eignex.koblas.Workspace? = null,
    ): N {
        nbrNodes.addAndFetch(1)
        val split = results.asSplitResult(node)
        if (split == null) {
            // newLeaf, not makeTerminalLeaf: a cloned leaf that cannot audit would freeze the merged
            // tree at the shape of whatever snapshot it adopted.
            val leaf = newLeaf(depth)
            shape.leafArm(leaf)?.merge(results.valueOf(node), workspace)
            return leaf
        }
        val pos = cloneFromResult(results.posOf(split), results, depth + 1, workspace)
        val neg = cloneFromResult(results.negOf(split), results, depth + 1, workspace)
        // Re-establish any orphan aggregate the snapshot encodes by comparing the recorded
        // value against the sum of the cloned children's aggregates.
        val residual = residualOf(split, results)
        val carry = if (residual.totalWeights > 0.0) shape.emptyArm().also { it.merge(residual, workspace) } else null
        return shape.makeSplitNode(results.splitOf(split), pos, neg, carry)
    }

    /** What a split snapshot's own aggregate holds beyond the sum of its two children's. */
    private fun <R : Any, RS : R> residualOf(split: RS, results: TreeResultShape<S, R, RS, P>): P {
        val childSum = shape.mergePayload(
            results.valueOf(results.posOf(split)),
            results.valueOf(results.negOf(split)),
        )
        return shape.subtractPayload(results.valueOf(split), childSum)
    }

    private fun foldIntoCarryover(node: SN, value: P, workspace: com.eignex.koblas.Workspace? = null) {
        if (value.totalWeights <= 0.0) return
        val existing = shape.carryoverOf(node)
        if (existing != null) {
            existing.merge(value, workspace)
        } else {
            shape.setCarryover(node, shape.emptyArm().also { it.merge(value, workspace) })
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
