package com.eignex.kumulant.stat.tree

import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.math.VectorView
import com.eignex.kumulant.stat.summary.WeightedVarianceResult

/**
 * Internal tree node. Every node — both internal splits and leaves — carries an arm
 * tracking observations that flowed through it. The leaf type is fixed to a weighted-
 * variance accumulator so the [SplitMetric] (currently [VarianceReduction]) can score
 * candidate splits; bandits dress this scalar predictor up with their own posterior
 * at score time.
 */
sealed class Node {
    /** Per-subtree weighted-variance arm. For leaves this is *the* arm; for splits it
     *  is the aggregate over every observation routed through the node. */
    abstract val arm: SeriesStat<WeightedVarianceResult>

    /** Walk to the leaf this row resolves to. */
    abstract fun findLeaf(row: VectorView): LeafNode
}

/** Routes by [split] to either [pos] (true) or [neg] (false). */
class SplitNode(
    /** Predicate routing observations into [pos] (true) or [neg] (false). */
    val split: Split,
    var pos: Node,
    var neg: Node,
    override val arm: SeriesStat<WeightedVarianceResult>,
) : Node() {
    override fun findLeaf(row: VectorView): LeafNode =
        if (split.direction(row)) pos.findLeaf(row) else neg.findLeaf(row)
}

/** Leaf node — terminus of the tree walk for a given row. */
sealed class LeafNode : Node() {
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

    /** Counter throttling audit work to every Nth observation. */
    var observationsSinceLastCheck: Int = 0
}
