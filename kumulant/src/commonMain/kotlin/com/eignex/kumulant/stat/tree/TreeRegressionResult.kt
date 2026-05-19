package com.eignex.kumulant.stat.tree

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.math.VectorView
import com.eignex.kumulant.stat.summary.WeightedVarianceResult

/**
 * Immutable snapshot of a [Tree] at read time. Carries the tree structure (split
 * predicates + per-node weighted-variance aggregates) so callers can route a context
 * vector to its leaf without reaching back into the live stat.
 *
 * The root's [WeightedVarianceResult] is exposed as the canonical scalar snapshot;
 * callers wanting context-specific predictions use [findLeaf] or [predict].
 */
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
sealed interface TreeNodeResult {
    /** Aggregate over every observation routed through this node. */
    val value: WeightedVarianceResult

    /** Route [x] to the leaf this subtree assigns it to. */
    fun findLeaf(x: VectorView): WeightedVarianceResult
}

/** Immutable split-node snapshot. */
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
data class TreeLeafResult(
    override val value: WeightedVarianceResult,
) : TreeNodeResult {
    override fun findLeaf(x: VectorView): WeightedVarianceResult = value
}

/** Freeze a live tree node into an immutable snapshot. */
fun Node.snapshot(): TreeNodeResult = when (this) {
    is SplitNode -> TreeSplitResult(split, pos.snapshot(), neg.snapshot(), arm.read(0L))
    is LeafNode -> TreeLeafResult(arm.read(0L))
}
