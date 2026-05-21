@file:OptIn(ExperimentalAtomicApi::class)

package com.eignex.kumulant.stat.tree

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.math.VectorView
import com.eignex.kumulant.stat.summary.VarianceStat
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import com.eignex.kumulant.stream.Mutex
import com.eignex.kumulant.stream.NoopMutex
import com.eignex.kumulant.stream.PlatformMutex
import kotlin.concurrent.Volatile
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.math.ln
import kotlin.math.pow
import kotlin.math.sqrt
import kotlin.random.Random

/**
 * Online VFDT-style decision tree partitioning context vectors. Each leaf carries a
 * weighted-variance accumulator; audit leaves additionally track pos/neg sub-arms per
 * candidate split and, every [TreeConfig.splitPeriod] observations, evaluate them against
 * the Hoeffding bound to decide whether to convert themselves into a [SplitNode].
 *
 * Internal split nodes hold no live arm — subtree aggregates (`rootSnapshot`, the `value`
 * fields on [TreeSplitResult]) are derived by combining descendants at snapshot/merge
 * time. The hot update path therefore touches exactly one arm: the leaf the observation
 * routes to. Under [Concurrency.Strict] this removes the root-arm serialization point
 * that previously bottlenecked multi-threaded throughput.
 *
 * Concurrency: leaf-arm updates run lock-free (the arms themselves honour [concurrency]).
 * The split-conversion path — the only one that mutates tree structure — is serialised
 * by a single per-tree lock, fired only every [TreeConfig.splitPeriod] observations per
 * audit leaf. Pointer writes on the hot path are skipped when the child reference is
 * unchanged, so the typical update is pure arm arithmetic.
 */
class Tree(
    private val splitCandidates: List<Split>,
    private val config: TreeConfig = TreeConfig(),
    private val concurrency: Concurrency = Concurrency.None,
    private val leafArmFactory: () -> SeriesStat<WeightedVarianceResult> = { VarianceStat(concurrency) },
    randomSeed: Int = 0,
) {
    private val random = Random(randomSeed)
    private val canGrow: Boolean = splitCandidates.isNotEmpty()
    private val splitLock: Mutex = if (concurrency == Concurrency.None) NoopMutex else PlatformMutex()

    private val nbrNodes: AtomicInt = AtomicInt(1)

    @Volatile
    private var root: Node = newLeaf(depth = 0)

    /** Walk to the leaf [row] resolves to. */
    fun findLeaf(row: VectorView): LeafNode = root.findLeaf(row)

    /** Live root node, for snapshotting. */
    fun rootNode(): Node = root

    /** Mean of the leaf [row] resolves to. */
    fun predict(row: VectorView): Double = findLeaf(row).arm.read(0L).mean

    /** Number of internal + leaf nodes currently in the tree. */
    val nodeCount: Int get() = nbrNodes.load()

    /** Fold an observation into the tree, possibly growing it. */
    fun update(row: VectorView, value: Double, weight: Double = 1.0) {
        val current = root
        val next = updateNode(current, row, value, weight, depth = 0)
        if (next !== current) root = next
    }

    /** Aggregate snapshot at the root — derived by walking leaves and any
     *  [SplitNode.carryover] aggregates. Under concurrent updates with active growth,
     *  pointer races at split-time can leak observations into orphaned sub-arms; this
     *  walk is therefore best-effort and may drift by a few ULPs of the configured
     *  workload under contention. Single-threaded runs are exact. */
    fun rootSnapshot(): WeightedVarianceResult = root.subtreeAggregate()

    /** Reset to a single fresh leaf. */
    fun reset() {
        splitLock.withLock {
            nbrNodes.store(1)
            root = newLeaf(depth = 0)
        }
    }

    /** Render the tree as nested `if (split) { ... } else { ... }` text. */
    fun prettyPrint(indent: String = ""): String = buildString { prettyPrintTo(this, root, indent) }

    /**
     * Structurally merge [other] into this tree. [other] is consumed (its node references
     * may be grafted into this tree) and must not be used afterwards.
     *
     *  - **Same split predicate**: recurse on both children — internal aggregates are
     *    derived from leaves, so no per-split merge step is needed.
     *  - **Both leaves**: merge arms directly.
     *  - **Self leaf, other split**: adopt other's structure wholesale and fold self's
     *    leaf aggregate into the adopted subtree's leftmost leaf.
     *  - **Self split, other leaf** *or* **different splits**: keep self's structure
     *    and fold other's aggregate (recursively combined if other is a split) into
     *    self's leftmost leaf.
     *
     * The "leftmost leaf" rule preserves the merged total weight but biases the
     * un-routable observations into a single bucket — an honest fallback when the
     * structures don't align.
     */
    fun merge(other: Tree) {
        splitLock.withLock {
            root = mergeNodes(root, other.root)
            nbrNodes.store(countNodes(root))
        }
    }

    /** Snapshot merge using only the immutable result. Falls through to the same rules
     *  as [merge] but the "other" side is a [TreeNodeResult] tree-of-results rather than
     *  a live Tree. */
    fun mergeSnapshot(other: TreeNodeResult) {
        splitLock.withLock {
            root = mergeNodeWithResult(root, other)
            nbrNodes.store(countNodes(root))
        }
    }

    private fun mergeNodes(a: Node, b: Node): Node {
        if (a is SplitNode && b is SplitNode && a.split == b.split) {
            a.pos = mergeNodes(a.pos, b.pos)
            a.neg = mergeNodes(a.neg, b.neg)
            val bCarry = b.carryover
            if (bCarry != null) foldIntoCarryover(a, bCarry.read(0L))
            return a
        }
        if (a is LeafNode && b is LeafNode) {
            a.arm.merge(b.arm.read(0L))
            return a
        }
        if (a is LeafNode && b is SplitNode) {
            foldIntoCarryover(b, a.arm.read(0L))
            return b
        }
        // a split + b leaf, or splits differ: keep a's structure, fold b's aggregate.
        foldIntoCarryover(a as SplitNode, b.subtreeAggregate())
        return a
    }

    private fun mergeNodeWithResult(a: Node, b: TreeNodeResult): Node {
        if (a is SplitNode && b is TreeSplitResult && a.split == b.split) {
            a.pos = mergeNodeWithResult(a.pos, b.pos)
            a.neg = mergeNodeWithResult(a.neg, b.neg)
            // b.value carries the full subtree aggregate; the child recursion already
            // folded the structurally-aligned portion. Pull only the residual — what b's
            // value holds beyond the sum of its children — into a's carryover.
            val childSum = mergeWVR(b.pos.value, b.neg.value)
            val residual = subtractWVR(b.value, childSum)
            if (residual.totalWeights > 0.0) foldIntoCarryover(a, residual)
            return a
        }
        if (a is LeafNode && b is TreeLeafResult) {
            a.arm.merge(b.value)
            return a
        }
        if (a is LeafNode && b is TreeSplitResult) {
            val cloned = cloneFromResult(b, depth = 0) as SplitNode
            foldIntoCarryover(cloned, a.arm.read(0L))
            return cloned
        }
        // a split + b leaf, or splits differ: keep a's structure, fold b's aggregate.
        foldIntoCarryover(a as SplitNode, b.value)
        return a
    }

    private fun foldIntoCarryover(node: SplitNode, value: WeightedVarianceResult) {
        if (value.totalWeights <= 0.0) return
        val existing = node.carryover
        if (existing != null) {
            existing.merge(value)
        } else {
            val arm = leafArmFactory()
            arm.merge(value)
            node.carryover = arm
        }
    }

    private fun cloneFromResult(node: TreeNodeResult, depth: Int): Node {
        nbrNodes.addAndFetch(1)
        return when (node) {
            is TreeLeafResult -> {
                val arm = leafArmFactory()
                arm.merge(node.value)
                TerminalLeaf(arm)
            }
            is TreeSplitResult -> {
                val pos = cloneFromResult(node.pos, depth + 1)
                val neg = cloneFromResult(node.neg, depth + 1)
                // Re-establish any orphan aggregate the snapshot encodes by comparing the
                // recorded value against the sum of the cloned children's aggregates.
                val childSum = mergeWVR(node.pos.value, node.neg.value)
                val residual = subtractWVR(node.value, childSum)
                val carry = if (residual.totalWeights > 0.0) {
                    leafArmFactory().also { it.merge(residual) }
                } else null
                SplitNode(split = node.split, pos = pos, neg = neg, carryover = carry)
            }
        }
    }

    private fun countNodes(node: Node): Int = when (node) {
        is SplitNode -> 1 + countNodes(node.pos) + countNodes(node.neg)
        is LeafNode -> 1
    }

    private fun prettyPrintTo(sb: StringBuilder, node: Node, indent: String) {
        when (node) {
            is SplitNode -> {
                sb.append(indent).append("if (").append(node.split.toString()).append(") {\n")
                prettyPrintTo(sb, node.pos, "$indent  ")
                sb.append(indent).append("} else {\n")
                prettyPrintTo(sb, node.neg, "$indent  ")
                sb.append(indent).append("}\n")
            }
            is LeafNode -> {
                val mean = node.arm.read(0L).mean
                sb.append(indent).append("leaf mean=").append(mean).append('\n')
            }
        }
    }

    private fun newLeaf(depth: Int): LeafNode {
        if (depth >= config.maxDepth || nbrNodes.load() + 1 > config.maxNodes || !canGrow) {
            return TerminalLeaf(leafArmFactory())
        }
        val subset = pickCandidates()
        return AuditLeaf(
            arm = leafArmFactory(),
            candidates = subset,
            pos = List(subset.size) { leafArmFactory() },
            neg = List(subset.size) { leafArmFactory() },
        )
    }

    private fun pickCandidates(): List<Split> {
        val k = config.mtry ?: return splitCandidates
        if (k >= splitCandidates.size) return splitCandidates
        return splitCandidates.shuffled(random).take(k)
    }

    private fun updateNode(node: Node, row: VectorView, value: Double, weight: Double, depth: Int): Node =
        when (node) {
            is SplitNode -> {
                if (node.split.direction(row)) {
                    val child = node.pos
                    val next = updateNode(child, row, value, weight, depth + 1)
                    if (next !== child) node.pos = next
                } else {
                    val child = node.neg
                    val next = updateNode(child, row, value, weight, depth + 1)
                    if (next !== child) node.neg = next
                }
                node
            }
            is TerminalLeaf -> {
                node.arm.update(value, 0L, weight)
                node
            }
            is AuditLeaf -> updateAuditLeaf(node, row, value, weight, depth)
        }

    private fun updateAuditLeaf(
        leaf: AuditLeaf,
        row: VectorView,
        value: Double,
        weight: Double,
        depth: Int,
    ): Node {
        leaf.arm.update(value, 0L, weight)
        for ((i, split) in leaf.candidates.withIndex()) {
            if (split.direction(row)) {
                leaf.pos[i].update(value, 0L, weight)
            } else {
                leaf.neg[i].update(value, 0L, weight)
            }
        }
        val ticks = leaf.observationsSinceLastCheck.addAndFetch(1L)
        if (ticks < config.splitPeriod) return leaf

        return splitLock.withLock {
            // Double-check inside the lock: another thread may have already audited
            // (and reset the counter) or replaced this leaf.
            if (leaf.observationsSinceLastCheck.load() < config.splitPeriod) return@withLock leaf
            leaf.observationsSinceLastCheck.store(0L)

            val total = leaf.arm.read(0L)
            if (total.totalWeights < config.minSamplesSplit) return@withLock leaf
            val pos = leaf.pos.map { it.read(0L) }
            val neg = leaf.neg.map { it.read(0L) }
            val ranked = config.metric.rank(total, pos, neg, config.minSamplesSplit, config.minSamplesLeaf)
            if (ranked.bestIndex < 0 || ranked.top1 <= 0.0) return@withLock leaf

            val eps = hoeffdingBound(config.delta, total.totalWeights, depth, config.deltaDecay)
            val passesHoeffding = ranked.top1 - ranked.top2 > eps
            val passesTau = eps < config.tau
            if (!passesHoeffding && !passesTau) return@withLock leaf

            // Stash the pre-split aggregate as the new split's carryover so it shows up
            // in subtree aggregates without burdening the hot path. New leaves start
            // empty so prior-driven Thompson/UCB exploration behaves as it did before
            // the split fired.
            nbrNodes.addAndFetch(2)
            SplitNode(
                split = leaf.candidates[ranked.bestIndex],
                pos = newLeaf(depth + 1),
                neg = newLeaf(depth + 1),
                carryover = leaf.arm,
            )
        }
    }

    private fun hoeffdingBound(delta: Double, n: Double, depth: Int, decay: Double): Double {
        if (n <= 0.0) return Double.POSITIVE_INFINITY
        val adjusted = delta * decay.pow(depth.toDouble())
        return sqrt(-ln(adjusted) / (2.0 * n))
    }
}
