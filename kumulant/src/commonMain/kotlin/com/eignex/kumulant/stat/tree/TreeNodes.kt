@file:OptIn(ExperimentalAtomicApi::class)

package com.eignex.kumulant.stat.tree

import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.math.VectorView
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.concurrent.Volatile
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi

/**
 * Internal tree node. The hot update path touches only the leaf an observation routes
 * to — internal split nodes are never written by [Tree.update]. Splits may carry an
 * optional *carryover* arm: a one-shot snapshot of the pre-split aggregate captured at
 * the moment a leaf converts into a split, plus any orphaned aggregates folded in by
 * mixed-structure merges. Subtree aggregates include the carryover but the hot path
 * never reads or writes it.
 */
sealed class Node {
    /** Walk to the leaf this row resolves to. */
    abstract fun findLeaf(row: VectorView): LeafNode
}

/**
 * Routes by [split] to either [pos] (true) or [neg] (false). The optional [carryover]
 * holds aggregates that don't structurally belong to either child — the pre-split data
 * frozen at split time, or orphans absorbed from a mixed merge. Never written by the
 * update hot path; never read by [findLeaf] or `predict`; included by `subtreeAggregate`.
 */
class SplitNode(
    /** Predicate routing observations into [pos] (true) or [neg] (false). */
    val split: Split,
    pos: Node,
    neg: Node,
    carryover: SeriesStat<WeightedVarianceResult>? = null,
) : Node() {
    @Volatile
    var pos: Node = pos

    @Volatile
    var neg: Node = neg

    /** One-shot carry-over aggregate, or null if the split holds no orphan data.
     *  Mutated only under the owning [Tree]'s split lock during merges; volatile so
     *  concurrent snapshots see a consistent reference. */
    @Volatile
    var carryover: SeriesStat<WeightedVarianceResult>? = carryover

    override fun findLeaf(row: VectorView): LeafNode =
        if (split.direction(row)) pos.findLeaf(row) else neg.findLeaf(row)
}

/** Leaf node — terminus of the tree walk for a given row, and the only node type
 *  that owns a live accumulator. */
sealed class LeafNode : Node() {
    /** The leaf's weighted-variance accumulator. */
    abstract val arm: SeriesStat<WeightedVarianceResult>

    final override fun findLeaf(row: VectorView): LeafNode = this
}

/** Frozen leaf — no further splits will be considered. */
class TerminalLeaf(override val arm: SeriesStat<WeightedVarianceResult>) : LeafNode()

/**
 * Leaf that tracks per-candidate pos/neg stats. When a candidate clears the Hoeffding-
 * bound test, this leaf is replaced by a [SplitNode]. The candidate subset is per-leaf
 * — picked at leaf birth — so mtry-style random subspace selection lives at the leaf level.
 */
class AuditLeaf(
    override val arm: SeriesStat<WeightedVarianceResult>,
    /** Candidate splits being evaluated at this leaf. */
    val candidates: List<Split>,
    /** Per-candidate accumulator for observations that route true. */
    val pos: List<SeriesStat<WeightedVarianceResult>>,
    /** Per-candidate accumulator for observations that route false. */
    val neg: List<SeriesStat<WeightedVarianceResult>>,
) : LeafNode() {
    init {
        require(candidates.size == pos.size && pos.size == neg.size) {
            "candidates/pos/neg must align: ${candidates.size}/${pos.size}/${neg.size}"
        }
    }

    /** Counter throttling audit work to every Nth observation. Atomic so concurrent
     *  updaters don't lose ticks; the split decision is taken by the thread that
     *  crosses the period boundary, under the tree's split lock. */
    val observationsSinceLastCheck: AtomicLong = AtomicLong(0L)
}

/** Inverse of [mergeWVR]: given a merged aggregate [total] and one summand [part],
 *  recover the other summand. Used when reconstructing carryover aggregates from
 *  snapshots that encode the sum but not the orphan portion explicitly. Returns a
 *  zero-weight result when the inverse is degenerate. */
internal fun subtractWVR(total: WeightedVarianceResult, part: WeightedVarianceResult): WeightedVarianceResult {
    val wTotal = total.totalWeights
    val wPart = part.totalWeights
    val wOther = wTotal - wPart
    if (wOther <= 0.0) return WeightedVarianceResult(0.0, 0.0, 0.0)
    val meanOther = (wTotal * total.mean - wPart * part.mean) / wOther
    val sstTotal = total.variance * wTotal
    val sstPart = part.variance * wPart
    val delta = meanOther - part.mean
    val sstOther = sstTotal - sstPart - delta * delta * (wPart * wOther / wTotal)
    val varOther = if (sstOther > 0.0) sstOther / wOther else 0.0
    return WeightedVarianceResult(wOther, meanOther, varOther)
}

/** Chan-style parallel merge of two weighted-variance aggregates. Pure; allocation-free
 *  except for the returned [WeightedVarianceResult]. */
internal fun mergeWVR(a: WeightedVarianceResult, b: WeightedVarianceResult): WeightedVarianceResult {
    val w1 = a.totalWeights
    val w2 = b.totalWeights
    if (w2 == 0.0) return a
    if (w1 == 0.0) return b
    val w = w1 + w2
    val delta = b.mean - a.mean
    val mean = a.mean + delta * (w2 / w)
    val sst = a.variance * w1 + b.variance * w2 + (delta * delta) * (w1 * w2 / w)
    return WeightedVarianceResult(w, mean, sst / w)
}

/** Recursive subtree aggregate — for leaves the arm's snapshot, for splits the merge of
 *  both children's aggregates plus any [SplitNode.carryover]. Called only on snapshot
 *  and merge paths, never on hot updates. */
internal fun Node.subtreeAggregate(): WeightedVarianceResult = when (this) {
    is LeafNode -> arm.read(0L)
    is SplitNode -> {
        val base = mergeWVR(pos.subtreeAggregate(), neg.subtreeAggregate())
        val carry = carryover
        if (carry != null) mergeWVR(base, carry.read(0L)) else base
    }
}
