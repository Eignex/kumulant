package com.eignex.kumulant.stat.regression.tree

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Classification analogue of [SplitMetric]: scores a candidate split using the
 * leaf's per-class counts. Higher is better; returns 0 when the split has no
 * signal (one side empty, or zero impurity reduction).
 *
 * Ships [GiniReduction] and [InformationGain] only; other criteria can land here
 * when consumers ask for them.
 */
@Serializable
sealed interface ClassificationSplitMetric {
    /** Score a candidate split given the leaf's total / pos / neg class counts. */
    fun score(total: ClassCountsResult, pos: ClassCountsResult, neg: ClassCountsResult): Double
}

/** Weighted Gini-impurity reduction. The classic CART classification criterion. */
@Serializable
@SerialName("GiniReduction")
data object GiniReduction : ClassificationSplitMetric {
    override fun score(total: ClassCountsResult, pos: ClassCountsResult, neg: ClassCountsResult): Double {
        val wPos = pos.totalWeights
        val wNeg = neg.totalWeights
        val w = wPos + wNeg
        if (w <= 0.0) return 0.0
        val weighted = (wPos / w) * pos.gini + (wNeg / w) * neg.gini
        return total.gini - weighted
    }
}

/** Information-gain split criterion: parent entropy minus weighted children entropy. */
@Serializable
@SerialName("InformationGain")
data object InformationGain : ClassificationSplitMetric {
    override fun score(total: ClassCountsResult, pos: ClassCountsResult, neg: ClassCountsResult): Double {
        val wPos = pos.totalWeights
        val wNeg = neg.totalWeights
        val w = wPos + wNeg
        if (w <= 0.0) return 0.0
        val weighted = (wPos / w) * pos.entropy + (wNeg / w) * neg.entropy
        return total.entropy - weighted
    }
}

/**
 * Mirror of [SplitMetric.rank] for classification: score every candidate and
 * return the top-two scores + index of the winner.
 */
internal fun ClassificationSplitMetric.rank(
    total: ClassCountsResult,
    pos: List<ClassCountsResult>,
    neg: List<ClassCountsResult>,
    minSamplesSplit: Double,
    minSamplesLeaf: Double,
): SplitInfo {
    require(pos.size == neg.size) { "pos and neg lists must align: ${pos.size} vs ${neg.size}" }
    var top1 = 0.0
    var top2 = 0.0
    var bestI = -1
    for (i in pos.indices) {
        val wPos = pos[i].totalWeights
        val wNeg = neg[i].totalWeights
        if (wPos < minSamplesLeaf || wNeg < minSamplesLeaf || wPos + wNeg < minSamplesSplit) continue
        val v = score(total, pos[i], neg[i])
        when {
            v > top1 -> {
                top2 = top1
                top1 = v
                bestI = i
            }

            v > top2 -> top2 = v
        }
    }
    return SplitInfo(top1, top2, bestI)
}
