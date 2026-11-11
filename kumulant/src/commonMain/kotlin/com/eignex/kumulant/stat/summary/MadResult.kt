package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.HasCenterScale
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.stat.quantile.TDigestResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Median and unscaled median absolute deviation, optionally retaining merge evidence. */
@Serializable
@SerialName("MadResult")
data class MadResult(
    /** Estimated stream median. */
    val median: Double,
    /** Estimated median of absolute deviations from [median]. */
    val mad: Double,
    /** Complete raw-value digest for merging; null for summary-only or frugal results. */
    val digest: TDigestResult? = null,
) : Result,
    HasCenterScale {
    override val center: Double get() = median
    override val scale: Double get() = mad
}

/**
 * Median and unscaled MAD of this snapshot's weighted centroid distribution.
 * Every deviation uses the same median. Centroid compression loses within-centroid
 * spread, so both estimates approximate the original observations. At an exact
 * half-weight boundary the two adjacent values are averaged.
 *
 * Uses O(k) time and O(1) working memory for k sorted centroids. Precomputed
 * quantiles are not required. The result retains this snapshot as merge evidence.
 *
 * @throws IllegalArgumentException if centroid means and weights have different sizes.
 */
fun TDigestResult.medianAbsoluteDeviation(): MadResult {
    require(means.size == weights.size) { "centroid means and weights must have the same size" }
    val total = weights.sum()
    if (means.isEmpty() || total <= 0.0 || means.any { it.isNaN() }) return MadResult(Double.NaN, Double.NaN, this)
    val halfWeight = total / 2.0
    val median = centroidMedian(halfWeight)
    if (median.isNaN()) return MadResult(Double.NaN, Double.NaN, this)

    var left = means.indexOfLast { it <= median }
    var right = left + 1
    var cumulative = 0.0
    var previous = Double.NaN
    while (left >= 0 || right < means.size) {
        val takeLeft = left >= 0 && (right >= means.size || median - means[left] <= means[right] - median)
        val index = if (takeLeft) left-- else right++
        val weight = weights[index]
        if (weight <= 0.0) continue
        val distance = if (takeLeft) median - means[index] else means[index] - median
        if (cumulative > 0.0 && cumulative == halfWeight) {
            return MadResult(median, previous / 2.0 + distance / 2.0, this)
        }
        cumulative += weight
        if (cumulative > halfWeight) return MadResult(median, distance, this)
        previous = distance
    }
    return MadResult(median, previous, this)
}

private fun TDigestResult.centroidMedian(halfWeight: Double): Double {
    var cumulative = 0.0
    var previous = Double.NaN
    for (i in means.indices) {
        val weight = weights[i]
        if (weight <= 0.0) continue
        if (cumulative > 0.0 && cumulative == halfWeight) return previous / 2.0 + means[i] / 2.0
        cumulative += weight
        if (cumulative > halfWeight) return means[i]
        previous = means[i]
    }
    return previous
}
