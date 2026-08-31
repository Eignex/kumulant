package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.core.F64VectorLike
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Immutable, **wire-portable** snapshot of a [RegressionTree] over a dense [F64VectorLike]
 * context. Carries the tree structure ([SerializableSplit] predicates + per-node
 * weighted-variance aggregates) so callers can route a context vector to its leaf without
 * reaching back into the live stat.
 *
 * Serialization is only meaningful for the [F64VectorLike] feature representation (the splits
 * must be wire-portable); trees grown over other [Split] row types use the live
 * [RegressionTree] directly and are not snapshotted to this type.
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
    fun findLeaf(x: F64VectorLike): WeightedVarianceResult = root.findLeaf(x)

    /** Mean of the leaf the context resolves to. */
    fun predict(x: F64VectorLike): Double = findLeaf(x).mean

    /** Cumulative weight folded into the tree. */
    val totalWeights: Double get() = root.value.totalWeights

    /** Root aggregate mean (over every observation absorbed). */
    val rootMean: Double get() = root.value.mean
}

/** Snapshot of a single tree node; split or leaf. */
@Serializable
sealed interface TreeNodeResult {
    /** Aggregate over every observation routed through this node. */
    val value: WeightedVarianceResult

    /** Route [x] to the leaf this subtree assigns it to. */
    fun findLeaf(x: F64VectorLike): WeightedVarianceResult
}

/** Immutable split-node snapshot. */
@Serializable
@SerialName("TreeSplitResult")
data class TreeSplitResult(
    /** Routing predicate (wire-portable). */
    val split: SerializableSplit,
    /** Subtree taken when [split] is true. */
    val pos: TreeNodeResult,
    /** Subtree taken when [split] is false. */
    val neg: TreeNodeResult,
    override val value: WeightedVarianceResult,
) : TreeNodeResult {
    override fun findLeaf(x: F64VectorLike): WeightedVarianceResult =
        if (split.direction(x)) pos.findLeaf(x) else neg.findLeaf(x)
}

/** Immutable leaf-node snapshot. */
@Serializable
@SerialName("TreeLeafResult")
data class TreeLeafResult(override val value: WeightedVarianceResult) : TreeNodeResult {
    override fun findLeaf(x: F64VectorLike): WeightedVarianceResult = value
}

/**
 * Freeze a live [F64VectorLike] tree node into an immutable, serializable snapshot. Internal
 * split aggregates are derived from the snapshotted children so the wire format stays
 * stable even though live splits hold no arm.
 *
 * Snapshotting is `F64VectorLike`-only because the wire format requires [SerializableSplit];
 * a `F64VectorLike` tree is always grown from [SerializableSplit] candidates, so the cast on
 * each split is safe by construction.
 */
fun RegressionNode<F64VectorLike>.snapshot(): TreeNodeResult = when (this) {
    is RegressionSplitNode -> {
        val p = pos.snapshot()
        val n = neg.snapshot()
        val base = mergeWVR(p.value, n.value)
        val carry = carryover
        val value = if (carry != null) mergeWVR(base, carry.read(0L)) else base
        TreeSplitResult(split as SerializableSplit, p, n, value)
    }

    is RegressionLeafNode -> TreeLeafResult(arm.read(0L))
}

/**
 * Snapshot merge using only the immutable result. Mirrors [RegressionTree.merge] but the
 * "other" side is a [TreeNodeResult] tree-of-results rather than a live tree. `F64VectorLike`
 * only, for the same reason [snapshot] is.
 *
 * An extension rather than a member because it exists only at `Row = F64VectorLike`, which a member of a
 * generic class cannot be constrained to. The algorithm is [TreeGrowth.mergeSnapshot].
 */
fun RegressionTree<F64VectorLike>.mergeSnapshot(other: TreeNodeResult) {
    growth.mergeSnapshot(other, RegressionResultShape)
}

/** Reads the immutable regression snapshot hierarchy on the growth engine's behalf. */
private object RegressionResultShape :
    TreeResultShape<Split<F64VectorLike>, TreeNodeResult, TreeSplitResult, WeightedVarianceResult> {
    override fun asSplitResult(node: TreeNodeResult): TreeSplitResult? = node as? TreeSplitResult

    override fun splitOf(node: TreeSplitResult): Split<F64VectorLike> = node.split

    override fun posOf(node: TreeSplitResult): TreeNodeResult = node.pos

    override fun negOf(node: TreeSplitResult): TreeNodeResult = node.neg

    override fun valueOf(node: TreeNodeResult): WeightedVarianceResult = node.value
}
