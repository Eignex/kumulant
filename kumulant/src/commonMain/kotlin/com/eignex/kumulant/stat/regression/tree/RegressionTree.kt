@file:OptIn(ExperimentalAtomicApi::class)

package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stat.summary.VarianceStat
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi

/*
 * The regression side's binding of the shared growth engine. Everything the tree does structurally lives
 * in [TreeGrowth]; this file supplies the node-shape SPI over the regression node hierarchy and a public
 * facade that forwards to it.
 */

/** The regression instantiation of the node-shape SPI. */
internal typealias RegressionShapeSpi<Row> = TreeShape<
    Row,
    Split<Row>,
    RegressionNode<Row>,
    RegressionSplitNode<Row>,
    RegressionAuditLeaf<Row>,
    WeightedVarianceResult,
    >

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
 * The growth algorithm itself is shared with [ClassificationTree]; both are thin facades over
 * [TreeGrowth], which they parameterise by their own node types.
 *
 * Concurrency: leaf-arm updates run lock-free (the arms themselves honour `concurrency`).
 * The split-conversion path; the only one that mutates tree structure; is serialised
 * by a single per-tree lock, fired only every [RegressionTreeConfig.splitPeriod] observations per
 * audit leaf. Pointer writes on the hot path are skipped when the child reference is
 * unchanged, so the typical update is pure arm arithmetic.
 */
class RegressionTree<Row>(
    splitCandidates: List<Split<Row>>,
    config: RegressionTreeConfig = RegressionTreeConfig(),
    concurrency: Concurrency = Concurrency.None,
    leafArmFactory: () -> SeriesStat<WeightedVarianceResult> = { VarianceStat(concurrency) },
    randomSeed: Int = 0,
) {
    internal val growth = TreeGrowth(
        shape = RegressionShape<Row>(config, leafArmFactory),
        splitCandidates = splitCandidates,
        config = config,
        concurrency = concurrency,
        randomSeed = randomSeed,
    )

    /** Walk to the leaf [row] resolves to. */
    fun findLeaf(row: Row): RegressionLeafNode<Row> = growth.root.findLeaf(row)

    /** Live root node, for snapshotting. */
    fun rootNode(): RegressionNode<Row> = growth.root

    /** Mean of the leaf [row] resolves to. */
    fun predict(row: Row): Double = findLeaf(row).arm.read(0L).mean

    /** Number of internal + leaf nodes currently in the tree. */
    val nodeCount: Int get() = growth.nbrNodes.load()

    /** Fold an observation into the tree, possibly growing it. */
    fun update(row: Row, value: Double, weight: Double = 1.0) = growth.update(row, value, weight)

    /** Aggregate snapshot at the root; derived by walking leaves and any
     *  [RegressionSplitNode.carryover] aggregates. Under concurrent updates with active growth,
     *  pointer races at split-time can leak observations into orphaned sub-arms; this
     *  walk is therefore best-effort and may drift by a few ULPs of the configured
     *  workload under contention. Single-threaded runs are exact. */
    fun rootSnapshot(): WeightedVarianceResult = growth.root.subtreeAggregate()

    /** Reset to a single fresh leaf. */
    fun reset() = growth.reset()

    /** Render the tree as nested `if (split) { ... } else { ... }` text. */
    fun prettyPrint(indent: String = ""): String = growth.prettyPrint(indent)

    /**
     * Structurally merge [other] into this tree. [other] is consumed (its node references
     * may be grafted into this tree) and must not be used afterwards. See [TreeGrowth.mergeTree]
     * for the alignment rules.
     */
    fun merge(other: RegressionTree<Row>) = growth.mergeTree(other.growth.root)
}

/** Reads and writes the regression node hierarchy on the growth engine's behalf. */
private class RegressionShape<Row>(
    private val config: RegressionTreeConfig,
    private val newArm: () -> SeriesStat<WeightedVarianceResult>,
) : RegressionShapeSpi<Row> {

    override fun asSplitNode(node: RegressionNode<Row>): RegressionSplitNode<Row>? = node as? RegressionSplitNode<Row>

    override fun asAuditLeaf(node: RegressionNode<Row>): RegressionAuditLeaf<Row>? = node as? RegressionAuditLeaf<Row>

    override fun leafArm(node: RegressionNode<Row>): SeriesStat<WeightedVarianceResult>? =
        (node as? RegressionLeafNode<Row>)?.arm

    override fun splitOf(node: RegressionSplitNode<Row>): Split<Row> = node.split

    override fun posOf(node: RegressionSplitNode<Row>): RegressionNode<Row> = node.pos

    override fun negOf(node: RegressionSplitNode<Row>): RegressionNode<Row> = node.neg

    override fun setPos(node: RegressionSplitNode<Row>, child: RegressionNode<Row>) {
        node.pos = child
    }

    override fun setNeg(node: RegressionSplitNode<Row>, child: RegressionNode<Row>) {
        node.neg = child
    }

    override fun carryoverOf(node: RegressionSplitNode<Row>): SeriesStat<WeightedVarianceResult>? = node.carryover

    override fun setCarryover(node: RegressionSplitNode<Row>, arm: SeriesStat<WeightedVarianceResult>) {
        node.carryover = arm
    }

    override fun candidatesOf(leaf: RegressionAuditLeaf<Row>): List<Split<Row>> = leaf.candidates

    override fun posArmsOf(leaf: RegressionAuditLeaf<Row>): List<SeriesStat<WeightedVarianceResult>> = leaf.pos

    override fun negArmsOf(leaf: RegressionAuditLeaf<Row>): List<SeriesStat<WeightedVarianceResult>> = leaf.neg

    override fun ticksOf(leaf: RegressionAuditLeaf<Row>): AtomicLong = leaf.observationsSinceLastCheck

    override fun makeSplitNode(
        split: Split<Row>,
        pos: RegressionNode<Row>,
        neg: RegressionNode<Row>,
        carryover: SeriesStat<WeightedVarianceResult>?,
    ): RegressionSplitNode<Row> = RegressionSplitNode(split, pos, neg, carryover)

    override fun makeTerminalLeaf(arm: SeriesStat<WeightedVarianceResult>): RegressionNode<Row> =
        RegressionTerminalLeaf(arm)

    override fun makeAuditLeaf(
        arm: SeriesStat<WeightedVarianceResult>,
        candidates: List<Split<Row>>,
        pos: List<SeriesStat<WeightedVarianceResult>>,
        neg: List<SeriesStat<WeightedVarianceResult>>,
    ): RegressionNode<Row> = RegressionAuditLeaf(arm, candidates, pos, neg)

    override fun emptyArm(): SeriesStat<WeightedVarianceResult> = newArm()

    override fun mergePayload(a: WeightedVarianceResult, b: WeightedVarianceResult): WeightedVarianceResult =
        mergeWVR(a, b)

    override fun subtractPayload(total: WeightedVarianceResult, part: WeightedVarianceResult): WeightedVarianceResult =
        subtractWVR(total, part)

    override fun aggregate(node: RegressionNode<Row>): WeightedVarianceResult = node.subtreeAggregate()

    override fun rank(
        total: WeightedVarianceResult,
        pos: List<WeightedVarianceResult>,
        neg: List<WeightedVarianceResult>,
    ): SplitInfo = config.metric.rank(total, pos, neg, config.minSamplesSplit, config.minSamplesLeaf)

    override fun leafLabel(arm: WeightedVarianceResult): String = "leaf mean=${arm.mean}"
}
