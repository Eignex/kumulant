package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.math.VectorView
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Immutable snapshot of a [RegressionTree] at read time. Carries the tree structure (split
 * predicates + per-node weighted-variance aggregates) so callers can route a context
 * vector to its leaf without reaching back into the live stat.
 *
 * The root's [WeightedVarianceResult] is exposed as the canonical scalar snapshot;
 * callers wanting context-specific predictions use [findLeaf] or [predict].
 */
@Serializable
@SerialName("TreeRegressionResult")
data class TreeRegressionResult(
    /** Root of the snapshot tree. */
    val root: TreeNodeResult,
) : Result {
    /** Walk to the leaf the context resolves to. */
    fun findLeaf(x: VectorView): WeightedVarianceResult = root.findLeaf(x)

    /** Mean of the leaf the context resolves to. */
    fun predict(x: VectorView): Double = findLeaf(x).mean

    /** Cumulative weight folded into the tree. */
    val totalWeights: Double get() = root.value.totalWeights

    /** Root aggregate mean (over every observation absorbed). */
    val rootMean: Double get() = root.value.mean
}

/** Snapshot of a single tree node — split or leaf. */
@Serializable
sealed interface TreeNodeResult {
    /** Aggregate over every observation routed through this node. */
    val value: WeightedVarianceResult

    /** Route [x] to the leaf this subtree assigns it to. */
    fun findLeaf(x: VectorView): WeightedVarianceResult
}

/** Immutable split-node snapshot. */
@Serializable
@SerialName("TreeSplitResult")
data class TreeSplitResult(
    /** Routing predicate. */
    val split: Split,
    /** Subtree taken when [split] is true. */
    val pos: TreeNodeResult,
    /** Subtree taken when [split] is false. */
    val neg: TreeNodeResult,
    override val value: WeightedVarianceResult,
) : TreeNodeResult {
    override fun findLeaf(x: VectorView): WeightedVarianceResult =
        if (split.direction(x)) pos.findLeaf(x) else neg.findLeaf(x)
}

/** Immutable leaf-node snapshot. */
@Serializable
@SerialName("TreeLeafResult")
data class TreeLeafResult(override val value: WeightedVarianceResult) : TreeNodeResult {
    override fun findLeaf(x: VectorView): WeightedVarianceResult = value
}

/** Freeze a live tree node into an immutable snapshot. Internal split aggregates are
 *  derived from the snapshotted children so the wire format stays stable even though
 *  live splits hold no arm. */
fun RegressionNode.snapshot(): TreeNodeResult = when (this) {
    is RegressionSplitNode -> {
        val p = pos.snapshot()
        val n = neg.snapshot()
        val base = mergeWVR(p.value, n.value)
        val carry = carryover
        val value = if (carry != null) mergeWVR(base, carry.read(0L)) else base
        TreeSplitResult(split, p, n, value)
    }

    is RegressionLeafNode -> TreeLeafResult(arm.read(0L))
}
