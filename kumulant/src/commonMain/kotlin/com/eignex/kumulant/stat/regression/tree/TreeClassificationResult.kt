package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.math.VectorView
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Classification mirror of [TreeRegressionResult]. */
@Serializable
@SerialName("TreeClassificationResult")
data class TreeClassificationResult(
    /** Root of the snapshot tree. */
    val root: TreeClassificationNodeResult,
) : Result {
    /** Walk to the leaf the context [x] resolves to and return its class-count snapshot. */
    fun findLeaf(x: VectorView): ClassCountsResult = root.findLeaf(x)

    /** Class probabilities at the leaf [x] resolves to. */
    fun probabilities(x: VectorView): DoubleArray = findLeaf(x).probabilities()

    /** Argmax class index at the leaf [x] resolves to. */
    fun predict(x: VectorView): Int = findLeaf(x).predict()

    /** Cumulative weight folded into the tree. */
    val totalWeights: Double get() = root.value.totalWeights

    /** Number of classes the tree is configured for. */
    val numClasses: Int get() = root.value.numClasses
}

/** Snapshot of a single classification-tree node — split or leaf. */
@Serializable
sealed interface TreeClassificationNodeResult {
    /** Aggregate over every observation routed through this node. */
    val value: ClassCountsResult

    /** Route [x] to the leaf this subtree assigns it to. */
    fun findLeaf(x: VectorView): ClassCountsResult
}

/** Immutable classification split-node snapshot. */
@Serializable
@SerialName("TreeClassificationSplitResult")
data class TreeClassificationSplitResult(
    /** Routing predicate. */
    val split: Split,
    /** Subtree taken when [split] is true. */
    val pos: TreeClassificationNodeResult,
    /** Subtree taken when [split] is false. */
    val neg: TreeClassificationNodeResult,
    override val value: ClassCountsResult,
) : TreeClassificationNodeResult {
    override fun findLeaf(x: VectorView): ClassCountsResult =
        if (split.direction(x)) pos.findLeaf(x) else neg.findLeaf(x)
}

/** Immutable classification leaf-node snapshot. */
@Serializable
@SerialName("TreeClassificationLeafResult")
data class TreeClassificationLeafResult(override val value: ClassCountsResult) : TreeClassificationNodeResult {
    override fun findLeaf(x: VectorView): ClassCountsResult = value
}

/** Freeze a live classification-tree node into an immutable snapshot. */
internal fun ClassificationNode.snapshot(): TreeClassificationNodeResult = when (this) {
    is ClassificationSplitNode -> {
        val p = pos.snapshot()
        val n = neg.snapshot()
        val base = mergeCC(p.value, n.value)
        val carry = carryover
        val value = if (carry != null) mergeCC(base, carry.read(0L)) else base
        TreeClassificationSplitResult(split, p, n, value)
    }

    is ClassificationLeafNode -> TreeClassificationLeafResult(arm.read(0L))
}
