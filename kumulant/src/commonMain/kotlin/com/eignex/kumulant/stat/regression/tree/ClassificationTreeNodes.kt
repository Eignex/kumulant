@file:OptIn(ExperimentalAtomicApi::class)

package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.VectorView
import com.eignex.kumulant.core.SeriesStat
import kotlin.concurrent.Volatile
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi

/**
 * Classification mirror of [RegressionNode]. Identical structure to the regression side except
 * that leaf arms carry class-count snapshots rather than weighted-variance summaries.
 */
sealed interface ClassificationNode {
    /** Walk to the leaf this row resolves to. */
    fun findLeaf(row: VectorView): ClassificationLeafNode
}

/** Split mirror; predicate routes to [pos] or [neg]; [carryover] absorbs orphan
 *  aggregates produced by mixed-structure merges or pre-split snapshots. */
class ClassificationSplitNode(
    /** Predicate routing observations into [pos] (true) or [neg] (false). */
    val split: SerializableSplit,
    pos: ClassificationNode,
    neg: ClassificationNode,
    carryover: SeriesStat<ClassCountsResult>? = null,
) : ClassificationNode {
    @Volatile
    var pos: ClassificationNode = pos

    @Volatile
    var neg: ClassificationNode = neg

    @Volatile
    var carryover: SeriesStat<ClassCountsResult>? = carryover

    override fun findLeaf(row: VectorView): ClassificationLeafNode =
        if (split.direction(row)) pos.findLeaf(row) else neg.findLeaf(row)
}

/** Leaf; owns a per-class count accumulator. */
sealed class ClassificationLeafNode : ClassificationNode {
    /** The leaf's class-count accumulator. */
    abstract val arm: SeriesStat<ClassCountsResult>
    final override fun findLeaf(row: VectorView): ClassificationLeafNode = this
}

/** Frozen leaf; no further splits considered. */
class ClassificationTerminalLeaf(override val arm: SeriesStat<ClassCountsResult>) : ClassificationLeafNode()

/** Audit leaf tracking per-candidate pos/neg class-count accumulators. */
class ClassificationAuditLeaf(
    override val arm: SeriesStat<ClassCountsResult>,
    /** Candidate splits being evaluated at this leaf. */
    val candidates: List<SerializableSplit>,
    /** Per-candidate accumulator for observations routing true. */
    val pos: List<SeriesStat<ClassCountsResult>>,
    /** Per-candidate accumulator for observations routing false. */
    val neg: List<SeriesStat<ClassCountsResult>>,
) : ClassificationLeafNode() {
    init {
        require(candidates.size == pos.size && pos.size == neg.size) {
            "candidates/pos/neg must align: ${candidates.size}/${pos.size}/${neg.size}"
        }
    }

    /** Atomic counter throttling audit work to every Nth observation. */
    val observationsSinceLastCheck: AtomicLong = AtomicLong(0L)
}

/** Recursive subtree aggregate; for leaves, the arm's snapshot; for splits, the
 *  element-wise merge of both children plus any carryover. */
internal fun ClassificationNode.subtreeAggregate(): ClassCountsResult = when (this) {
    is ClassificationLeafNode -> arm.read(0L)

    is ClassificationSplitNode -> {
        val base = mergeCC(pos.subtreeAggregate(), neg.subtreeAggregate())
        val carry = carryover
        if (carry != null) mergeCC(base, carry.read(0L)) else base
    }
}
