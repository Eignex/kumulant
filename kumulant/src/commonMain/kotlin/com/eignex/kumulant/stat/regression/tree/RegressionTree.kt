@file:OptIn(ExperimentalAtomicApi::class)

package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stat.summary.VarianceStat
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import com.eignex.kumulant.stream.Mutex
import com.eignex.kumulant.stream.NoopMutex
import com.eignex.kumulant.stream.PlatformMutex
import com.eignex.kumulant.stream.guarded
import kotlin.concurrent.Volatile
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.random.Random

/**
 * Online VFDT-style decision tree partitioning feature rows of type [Row]. Each leaf
 * carries a weighted-variance accumulator; audit leaves additionally track pos/neg
 * sub-arms per candidate split and, every [RegressionTreeConfig.splitPeriod] observations,
 * evaluate them against the Hoeffding bound to decide whether to convert themselves into
 * a [RegressionSplitNode].
 *
 * The engine is **generic over the feature representation**: it only ever inspects a row
 * by calling [Split.direction], so any feature type can drive growth by supplying its own
 * [Split]s (e.g. a dense [com.eignex.koblas.VectorView] for the built-in stats, or
 * a typed/constraint-coupled row from a downstream library). Wire-portable serialization
 * of a snapshot is only meaningful for the [com.eignex.koblas.VectorView] case and
 * lives in `TreeRegressionResult.kt` as `VectorView`-constrained extensions.
 *
 * Internal split nodes hold no live arm; subtree aggregates (`rootSnapshot`, the `value`
 * fields on the snapshot results) are derived by combining descendants at snapshot/merge
 * time. The hot update path therefore touches exactly one arm: the leaf the observation
 * routes to.
 *
 * Concurrency: leaf-arm updates run lock-free (the arms themselves honour [concurrency]).
 * The split-conversion path; the only one that mutates tree structure; is serialised
 * by a single per-tree lock, fired only every [RegressionTreeConfig.splitPeriod] observations per
 * audit leaf. Pointer writes on the hot path are skipped when the child reference is
 * unchanged, so the typical update is pure arm arithmetic.
 */
class RegressionTree<Row>(
    private val splitCandidates: List<Split<Row>>,
    private val config: RegressionTreeConfig = RegressionTreeConfig(),
    private val concurrency: Concurrency = Concurrency.None,
    internal val leafArmFactory: () -> SeriesStat<WeightedVarianceResult> = { VarianceStat(concurrency) },
    randomSeed: Int = 0,
) {
    private val random = Random(randomSeed)
    private val canGrow: Boolean = splitCandidates.isNotEmpty()
    internal val splitLock: Mutex = if (concurrency == Concurrency.None) NoopMutex else PlatformMutex()

    internal val nbrNodes: AtomicInt = AtomicInt(1)

    @Volatile
    internal var root: RegressionNode<Row> = newLeaf(depth = 0)

    /** Walk to the leaf [row] resolves to. */
    fun findLeaf(row: Row): RegressionLeafNode<Row> = root.findLeaf(row)

    /** Live root node, for snapshotting. */
    fun rootNode(): RegressionNode<Row> = root

    /** Mean of the leaf [row] resolves to. */
    fun predict(row: Row): Double = findLeaf(row).arm.read(0L).mean

    /** Number of internal + leaf nodes currently in the tree. */
    val nodeCount: Int get() = nbrNodes.load()

    /** Fold an observation into the tree, possibly growing it. */
    fun update(row: Row, value: Double, weight: Double = 1.0) {
        val current = root
        val next = updateNode(current, row, value, weight, depth = 0)
        if (next !== current) root = next
    }

    /** Aggregate snapshot at the root; derived by walking leaves and any
     *  [RegressionSplitNode.carryover] aggregates. Under concurrent updates with active growth,
     *  pointer races at split-time can leak observations into orphaned sub-arms; this
     *  walk is therefore best-effort and may drift by a few ULPs of the configured
     *  workload under contention. Single-threaded runs are exact. */
    fun rootSnapshot(): WeightedVarianceResult = root.subtreeAggregate()

    /** Reset to a single fresh leaf. */
    fun reset() {
        splitLock.guarded {
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
     *  - **Same split predicate**: recurse on both children; internal aggregates are
     *    derived from leaves, so no per-split merge step is needed.
     *  - **Both leaves**: merge arms directly.
     *  - **Self leaf, other split**: adopt other's structure wholesale and fold self's
     *    leaf aggregate into the adopted subtree's leftmost leaf.
     *  - **Self split, other leaf** *or* **different splits**: keep self's structure
     *    and fold other's aggregate (recursively combined if other is a split) into
     *    self's leftmost leaf.
     *
     * The "leftmost leaf" rule preserves the merged total weight but biases the
     * un-routable observations into a single bucket; an honest fallback when the
     * structures don't align.
     */
    fun merge(other: RegressionTree<Row>) {
        splitLock.guarded {
            root = mergeNodes(root, other.root)
            nbrNodes.store(countNodes(root))
        }
    }

    private fun mergeNodes(a: RegressionNode<Row>, b: RegressionNode<Row>): RegressionNode<Row> {
        if (a is RegressionSplitNode && b is RegressionSplitNode && a.split == b.split) {
            a.pos = mergeNodes(a.pos, b.pos)
            a.neg = mergeNodes(a.neg, b.neg)
            val bCarry = b.carryover
            if (bCarry != null) foldIntoCarryover(a, bCarry.read(0L))
            return a
        }
        if (a is RegressionLeafNode && b is RegressionLeafNode) {
            a.arm.merge(b.arm.read(0L))
            return a
        }
        if (a is RegressionLeafNode && b is RegressionSplitNode) {
            foldIntoCarryover(b, a.arm.read(0L))
            return b
        }
        // a split + b leaf, or splits differ: keep a's structure, fold b's aggregate.
        foldIntoCarryover(a as RegressionSplitNode, b.subtreeAggregate())
        return a
    }

    internal fun foldIntoCarryover(node: RegressionSplitNode<Row>, value: WeightedVarianceResult) {
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

    internal fun countNodes(node: RegressionNode<Row>): Int = when (node) {
        is RegressionSplitNode -> 1 + countNodes(node.pos) + countNodes(node.neg)
        is RegressionLeafNode -> 1
    }

    private fun prettyPrintTo(sb: StringBuilder, node: RegressionNode<Row>, indent: String) {
        when (node) {
            is RegressionSplitNode -> {
                sb.append(indent).append("if (").append(node.split.toString()).append(") {\n")
                prettyPrintTo(sb, node.pos, "$indent  ")
                sb.append(indent).append("} else {\n")
                prettyPrintTo(sb, node.neg, "$indent  ")
                sb.append(indent).append("}\n")
            }

            is RegressionLeafNode -> {
                val mean = node.arm.read(0L).mean
                sb.append(indent).append("leaf mean=").append(mean).append('\n')
            }
        }
    }

    private fun newLeaf(depth: Int): RegressionLeafNode<Row> {
        if (depth >= config.maxDepth || nbrNodes.load() + 1 > config.maxNodes || !canGrow) {
            return RegressionTerminalLeaf(leafArmFactory())
        }
        val subset = splitCandidates.pickCandidates(config.mtry, random)
        return RegressionAuditLeaf(
            arm = leafArmFactory(),
            candidates = subset,
            pos = List(subset.size) { leafArmFactory() },
            neg = List(subset.size) { leafArmFactory() },
        )
    }

    private fun updateNode(
        node: RegressionNode<Row>,
        row: Row,
        value: Double,
        weight: Double,
        depth: Int,
    ): RegressionNode<Row> = when (node) {
        is RegressionSplitNode -> {
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

        is RegressionTerminalLeaf -> {
            node.arm.update(value, 0L, weight)
            node
        }

        is RegressionAuditLeaf -> updateAuditLeaf(node, row, value, weight, depth)
    }

    private fun updateAuditLeaf(
        leaf: RegressionAuditLeaf<Row>,
        row: Row,
        value: Double,
        weight: Double,
        depth: Int,
    ): RegressionNode<Row> {
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

        return splitLock.guarded {
            // Double-check inside the lock: another thread may have already audited
            // (and reset the counter) or replaced this leaf.
            if (leaf.observationsSinceLastCheck.load() < config.splitPeriod) return@guarded leaf
            leaf.observationsSinceLastCheck.store(0L)

            val total = leaf.arm.read(0L)
            val pos = leaf.pos.map { it.read(0L) }
            val neg = leaf.neg.map { it.read(0L) }
            val ranked = config.metric.rank(total, pos, neg, config.minSamplesSplit, config.minSamplesLeaf)
            // Every gate lives in shouldSplit now. Both trees spelled the three of them out, and the
            // Hoeffding-versus-tau disjunction in particular is the kind of condition that reads correct
            // in either copy while the two have quietly stopped meaning the same thing.
            if (!shouldSplit(ranked, total.totalWeights, depth, config)) return@guarded leaf

            // Stash the pre-split aggregate as the new split's carryover so it shows up
            // in subtree aggregates without burdening the hot path. New leaves start
            // empty so prior-driven Thompson/UCB exploration behaves as it did before
            // the split fired.
            nbrNodes.addAndFetch(2)
            RegressionSplitNode(
                split = leaf.candidates[ranked.bestIndex],
                pos = newLeaf(depth + 1),
                neg = newLeaf(depth + 1),
                carryover = leaf.arm,
            )
        }
    }
}
