@file:OptIn(ExperimentalAtomicApi::class)

package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.core.F64VectorLike
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.requireAtLeastTwoClasses
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi

/*
 * The classification side's binding of the shared growth engine, mirroring `RegressionTree.kt`: a
 * node-shape SPI over the classification node hierarchy, a snapshot-shape SPI over the classification
 * snapshot hierarchy, and a public facade that forwards to [TreeGrowth].
 */

/** The classification instantiation of the node-shape SPI. */
internal typealias ClassificationShapeSpi = TreeShape<
    F64VectorLike,
    SerializableSplit,
    ClassificationNode,
    ClassificationSplitNode,
    ClassificationAuditLeaf,
    ClassCountsResult,
    >

/**
 * Classification mirror of [RegressionTree]: online VFDT decision tree where each leaf carries
 * a per-class count accumulator and audit leaves track class counts per candidate
 * split. Splits fire when a candidate clears the Hoeffding bound on the configured
 * [ClassificationSplitMetric] (Gini or information gain).
 *
 * Both trees drive the same [TreeGrowth] engine, each parameterising it with its own node types.
 *
 * Concurrency model matches [RegressionTree]: lock-free leaf updates, single split-conversion
 * lock fired only at split-decision time.
 */
class ClassificationTree(
    private val numClasses: Int,
    splitCandidates: List<SerializableSplit>,
    config: ClassificationTreeConfig = ClassificationTreeConfig(),
    concurrency: Concurrency = Concurrency.None,
    leafArmFactory: () -> SeriesStat<ClassCountsResult> = { ClassCountsStat(numClasses, concurrency) },
    randomSeed: Int = 0,
) {
    init {
        requireAtLeastTwoClasses(numClasses)
    }

    private val growth = TreeGrowth(
        shape = ClassificationShape(config, leafArmFactory),
        splitCandidates = splitCandidates,
        config = config,
        concurrency = concurrency,
        randomSeed = randomSeed,
    )

    /** Walk to the leaf [row] resolves to. */
    fun findLeaf(row: F64VectorLike): ClassificationLeafNode = growth.root.findLeaf(row)

    /** Live root node, for snapshotting. */
    fun rootNode(): ClassificationNode = growth.root

    /** Argmax class at the leaf [row] resolves to. */
    fun predict(row: F64VectorLike): Int = findLeaf(row).arm.read(0L).predict()

    /** Probabilities at the leaf [row] resolves to. */
    fun probabilities(row: F64VectorLike): DoubleArray = findLeaf(row).arm.read(0L).probabilities()

    /** Number of internal + leaf nodes currently in the tree. */
    val nodeCount: Int get() = growth.nbrNodes.load()

    /** Fold an observation `(row, classLabel)` into the tree, possibly growing it. */
    fun update(row: F64VectorLike, classLabel: Int, weight: Double = 1.0) {
        if (classLabel !in 0 until numClasses) return
        growth.update(row, classLabel.toDouble(), weight)
    }

    /** Aggregate class-count snapshot at the root, walking leaves and split carryovers. */
    fun rootSnapshot(): ClassCountsResult = growth.root.subtreeAggregate()

    /** Reset to a single fresh leaf. */
    fun reset() = growth.reset()

    /** Render the tree as nested if-else text. */
    fun prettyPrint(indent: String = ""): String = growth.prettyPrint(indent)

    /** Structurally merge [other] into this tree; [other] is consumed. */
    fun merge(other: ClassificationTree) = growth.mergeTree(other.growth.root)

    /** Snapshot merge: same rules as [merge] but the other side is an immutable result. */
    fun mergeSnapshot(other: TreeClassificationNodeResult, workspace: com.eignex.koblas.Workspace? = null) =
        growth.mergeSnapshot(other, ClassificationResultShape, workspace)
}

/** Reads and writes the classification node hierarchy on the growth engine's behalf. */
private class ClassificationShape(
    private val config: ClassificationTreeConfig,
    private val newArm: () -> SeriesStat<ClassCountsResult>,
) : ClassificationShapeSpi {

    override fun asSplitNode(node: ClassificationNode): ClassificationSplitNode? = node as? ClassificationSplitNode

    override fun asAuditLeaf(node: ClassificationNode): ClassificationAuditLeaf? = node as? ClassificationAuditLeaf

    override fun leafArm(node: ClassificationNode): SeriesStat<ClassCountsResult>? =
        (node as? ClassificationLeafNode)?.arm

    override fun splitOf(node: ClassificationSplitNode): SerializableSplit = node.split

    override fun posOf(node: ClassificationSplitNode): ClassificationNode = node.pos

    override fun negOf(node: ClassificationSplitNode): ClassificationNode = node.neg

    override fun setPos(node: ClassificationSplitNode, child: ClassificationNode) {
        node.pos = child
    }

    override fun setNeg(node: ClassificationSplitNode, child: ClassificationNode) {
        node.neg = child
    }

    override fun carryoverOf(node: ClassificationSplitNode): SeriesStat<ClassCountsResult>? = node.carryover

    override fun setCarryover(node: ClassificationSplitNode, arm: SeriesStat<ClassCountsResult>) {
        node.carryover = arm
    }

    override fun candidatesOf(leaf: ClassificationAuditLeaf): List<SerializableSplit> = leaf.candidates

    override fun posArmsOf(leaf: ClassificationAuditLeaf): List<SeriesStat<ClassCountsResult>> = leaf.pos

    override fun negArmsOf(leaf: ClassificationAuditLeaf): List<SeriesStat<ClassCountsResult>> = leaf.neg

    override fun ticksOf(leaf: ClassificationAuditLeaf): AtomicLong = leaf.observationsSinceLastCheck

    override fun makeSplitNode(
        split: SerializableSplit,
        pos: ClassificationNode,
        neg: ClassificationNode,
        carryover: SeriesStat<ClassCountsResult>?,
    ): ClassificationSplitNode = ClassificationSplitNode(split, pos, neg, carryover)

    override fun makeTerminalLeaf(arm: SeriesStat<ClassCountsResult>): ClassificationNode =
        ClassificationTerminalLeaf(arm)

    override fun makeAuditLeaf(
        arm: SeriesStat<ClassCountsResult>,
        candidates: List<SerializableSplit>,
        pos: List<SeriesStat<ClassCountsResult>>,
        neg: List<SeriesStat<ClassCountsResult>>,
    ): ClassificationNode = ClassificationAuditLeaf(arm, candidates, pos, neg)

    override fun emptyArm(): SeriesStat<ClassCountsResult> = newArm()

    override fun mergePayload(a: ClassCountsResult, b: ClassCountsResult): ClassCountsResult = mergeCC(a, b)

    override fun subtractPayload(total: ClassCountsResult, part: ClassCountsResult): ClassCountsResult =
        subtractCC(total, part)

    override fun aggregate(node: ClassificationNode): ClassCountsResult = node.subtreeAggregate()

    override fun rank(
        total: ClassCountsResult,
        pos: List<ClassCountsResult>,
        neg: List<ClassCountsResult>,
    ): SplitInfo = config.metric.rank(total, pos, neg, config.minSamplesSplit, config.minSamplesLeaf)

    override fun leafLabel(arm: ClassCountsResult): String = "leaf counts=${arm.counts.toList()}"
}

/** Reads the immutable classification snapshot hierarchy on the growth engine's behalf. */
private object ClassificationResultShape : TreeResultShape<
    SerializableSplit,
    TreeClassificationNodeResult,
    TreeClassificationSplitResult,
    ClassCountsResult,
    > {
    override fun asSplitResult(node: TreeClassificationNodeResult): TreeClassificationSplitResult? =
        node as? TreeClassificationSplitResult

    override fun splitOf(node: TreeClassificationSplitResult): SerializableSplit = node.split

    override fun posOf(node: TreeClassificationSplitResult): TreeClassificationNodeResult = node.pos

    override fun negOf(node: TreeClassificationSplitResult): TreeClassificationNodeResult = node.neg

    override fun valueOf(node: TreeClassificationNodeResult): ClassCountsResult = node.value
}
