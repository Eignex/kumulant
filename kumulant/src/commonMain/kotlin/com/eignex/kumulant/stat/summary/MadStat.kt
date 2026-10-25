package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasCenterScale
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stat.StatDefaults
import com.eignex.kumulant.stat.quantile.TDigestResult
import com.eignex.kumulant.stat.quantile.TDigestStat
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

/**
 * Streaming median and unscaled median absolute deviation from one raw-value [TDigestStat].
 *
 * Updates accumulate observations without computing deviations. Reading computes the
 * weighted median of centroid locations, then the weighted median of their absolute
 * distances from that center. The sorted locations permit a linear outward scan.
 * Compression can lose within-centroid spread; this estimates the historical MAD,
 * without retaining deviations measured against earlier centers.
 *
 * Merge combines the complete digest retained by [MadResult.digest], then reads
 * recalculate both estimates. A summary-only result contributes unit total weight
 * split between `median - mad` and `median + mad`, approximating its missing evidence.
 *
 * **Use cases:** robust center and scale for heavy-tailed streams. Use [FrugalMadStat]
 * for constant-memory adaptive tracking with an equal-weight approximate merge.
 *
 * **Memory:** O([compression]); one digest and its update buffer.
 *
 * **Update:** Inherits [TDigestStat]'s buffered update cost; no snapshot per update.
 * Reading adds an O(k) scan with O(1) working memory for k centroids.
 *
 * **Concurrency:** Inherits [TDigestStat]'s concurrency model. Both estimates are
 * calculated from the same detached snapshot.
 */
class MadStat(
    /** T-digest compression; larger values retain more centroids and finer detail. */
    val compression: Double = StatDefaults.TDIGEST_COMPRESSION,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<MadResult> {

    private val valueDigest = TDigestStat(compression, doubleArrayOf(), concurrency)

    override fun update(value: Double, timestampNanos: Long, weight: Double) =
        valueDigest.update(value, timestampNanos, weight)

    override fun validateMerge(values: MadResult) {
        values.digest?.let { valueDigest.validateMerge(it) }
    }

    override fun merge(values: MadResult) {
        val digest = values.digest
        if (digest != null) {
            valueDigest.merge(digest)
        } else if (!values.median.isNaN()) {
            val deviation = if (values.mad.isNaN()) 0.0 else values.mad
            valueDigest.update(values.median - deviation, weight = 0.5)
            valueDigest.update(values.median + deviation, weight = 0.5)
        }
    }

    override fun reset() = valueDigest.reset()

    override fun read(timestampNanos: Long): MadResult = valueDigest.read(timestampNanos).medianAbsoluteDeviation()

    override fun create(concurrency: Concurrency?) = MadStat(compression, concurrency ?: this.concurrency)
}
