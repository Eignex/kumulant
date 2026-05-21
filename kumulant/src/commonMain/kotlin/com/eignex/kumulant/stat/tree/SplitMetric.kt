package com.eignex.kumulant.stat.tree

import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Scores a candidate split against a leaf's pre-split distribution. Higher is better.
 * Returned score must satisfy `value(total, total, empty) == 0` so that "no signal" is
 * always last in the ranking.
 *
 * Ships [VarianceReduction] only; other metrics (t-test, chi-square, info gain) can
 * be added as concrete callers ask for them. Sealed so the wire form (TreeConfig
 * inside a tree spec) stays portable.
 */
@Serializable
sealed interface SplitMetric {
    /** Score a candidate split given its total / pos / neg variance snapshots. */
    fun score(total: WeightedVarianceResult, pos: WeightedVarianceResult, neg: WeightedVarianceResult): Double
}

/** Mean variance reduction. The classic CART regression criterion. */
@Serializable
@SerialName("VarianceReduction")
data object VarianceReduction : SplitMetric {
    override fun score(
        total: WeightedVarianceResult,
        pos: WeightedVarianceResult,
        neg: WeightedVarianceResult,
    ): Double {
        val wPos = pos.totalWeights
        val wNeg = neg.totalWeights
        val w = wPos + wNeg
        if (w <= 0.0) return 0.0
        val weighted = (wPos / w) * pos.variance + (wNeg / w) * neg.variance
        return total.variance - weighted
    }
}

/** Result of evaluating all candidate splits at a leaf: best score, runner-up, best index. */
data class SplitInfo(
    /** Best candidate's score. */
    val top1: Double,
    /** Runner-up's score. */
    val top2: Double,
    /** Index into the leaf's candidate list of the best split (`-1` if none qualified). */
    val bestIndex: Int,
)

/**
 * Score every candidate split and return the top-2 + index. Splits that don't meet
 * [minSamplesLeaf] on both sides or [minSamplesSplit] in total are skipped.
 */
fun SplitMetric.rank(
    total: WeightedVarianceResult,
    pos: List<WeightedVarianceResult>,
    neg: List<WeightedVarianceResult>,
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
