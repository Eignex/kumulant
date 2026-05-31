@file:OptIn(ExperimentalAtomicApi::class)

package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.math.VectorView
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.concurrent.atomics.ExperimentalAtomicApi

/**
 * Immutable, **wire-portable** snapshot of a [RegressionTree] over a dense [VectorView]
 * context. Carries the tree structure ([SerializableSplit] predicates + per-node
 * weighted-variance aggregates) so callers can route a context vector to its leaf without
 * reaching back into the live stat.
 *
 * Serialization is only meaningful for the [VectorView] feature representation (the splits
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
    fun findLeaf(x: VectorView): WeightedVarianceResult = root.findLeaf(x)

    /** Mean of the leaf the context resolves to. */
    fun predict(x: VectorView): Double = findLeaf(x).mean

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
    fun findLeaf(x: VectorView): WeightedVarianceResult
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
    override fun findLeaf(x: VectorView): WeightedVarianceResult =
        if (split.direction(x)) pos.findLeaf(x) else neg.findLeaf(x)
}

/** Immutable leaf-node snapshot. */
@Serializable
@SerialName("TreeLeafResult")
data class TreeLeafResult(override val value: WeightedVarianceResult) : TreeNodeResult {
    override fun findLeaf(x: VectorView): WeightedVarianceResult = value
}

/**
 * Freeze a live [VectorView] tree node into an immutable, serializable snapshot. Internal
 * split aggregates are derived from the snapshotted children so the wire format stays
 * stable even though live splits hold no arm.
 *
 * Snapshotting is `VectorView`-only because the wire format requires [SerializableSplit];
 * a `VectorView` tree is always grown from [SerializableSplit] candidates, so the cast on
 * each split is safe by construction.
 */
fun RegressionNode<VectorView>.snapshot(): TreeNodeResult = when (this) {
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
 * "other" side is a [TreeNodeResult] tree-of-results rather than a live tree. `VectorView`
 * only, for the same reason [snapshot] is.
 */
fun RegressionTree<VectorView>.mergeSnapshot(other: TreeNodeResult) {
    splitLock.withLock {
        root = mergeNodeWithResult(root, other)
        nbrNodes.store(countNodes(root))
    }
}

private fun RegressionTree<VectorView>.mergeNodeWithResult(
    a: RegressionNode<VectorView>,
    b: TreeNodeResult,
): RegressionNode<VectorView> {
    if (a is RegressionSplitNode && b is TreeSplitResult && a.split == b.split) {
        a.pos = mergeNodeWithResult(a.pos, b.pos)
        a.neg = mergeNodeWithResult(a.neg, b.neg)
        // b.value carries the full subtree aggregate; the child recursion already
        // folded the structurally-aligned portion. Pull only the residual; what b's
        // value holds beyond the sum of its children; into a's carryover.
        val childSum = mergeWVR(b.pos.value, b.neg.value)
        val residual = subtractWVR(b.value, childSum)
        if (residual.totalWeights > 0.0) foldIntoCarryover(a, residual)
        return a
    }
    if (a is RegressionLeafNode && b is TreeLeafResult) {
        a.arm.merge(b.value)
        return a
    }
    if (a is RegressionLeafNode && b is TreeSplitResult) {
        val cloned = cloneFromResult(b, depth = 0) as RegressionSplitNode
        foldIntoCarryover(cloned, a.arm.read(0L))
        return cloned
    }
    // a split + b leaf, or splits differ: keep a's structure, fold b's aggregate.
    foldIntoCarryover(a as RegressionSplitNode, b.value)
    return a
}

private fun RegressionTree<VectorView>.cloneFromResult(node: TreeNodeResult, depth: Int): RegressionNode<VectorView> {
    nbrNodes.addAndFetch(1)
    return when (node) {
        is TreeLeafResult -> {
            val arm = leafArmFactory()
            arm.merge(node.value)
            RegressionTerminalLeaf(arm)
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
            } else {
                null
            }
            RegressionSplitNode(split = node.split, pos = pos, neg = neg, carryover = carry)
        }
    }
}
