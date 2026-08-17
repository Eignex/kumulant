package com.eignex.kumulant.stat.calibration

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasObservationCount
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.requirePositiveBins
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Snapshot from [IsotonicCalibratorStat]: a non-decreasing step function from
 * raw score in `[0, 1]` to calibrated probability, derived from a binned
 * `(positives, total)` histogram via Pool Adjacent Violators.
 *
 * The arrays are parallel and length `numBins`. [binMidpoints] are equal-width
 * centres over `[0, 1]`; [probabilities] are PAV-monotonised empirical
 * positive rates, with any empty bins carrying the rate of the nearest
 * non-empty pool.
 */
@Serializable
@SerialName("IsotonicCalibratorResult")
data class IsotonicCalibratorResult(
    /** Number of equal-width bins covering `[0, 1]`. */
    val numBins: Int,
    /** Midpoint of each bin; parallel to [probabilities]. */
    val binMidpoints: DoubleArray,
    /** Monotone-non-decreasing calibrated probability per bin. */
    val probabilities: DoubleArray,
    /** Cumulative observation weight folded in. */
    override val totalWeights: Double,
) : HasObservationCount {

    init {
        requirePositiveBins(numBins)
        require(binMidpoints.size == numBins && probabilities.size == numBins) {
            "binMidpoints and probabilities must have length $numBins"
        }
    }

    /**
     * Calibrated probability for raw score [x]. Out-of-range inputs are clamped
     * to `[0, 1]`; intermediate values are linearly interpolated between bin
     * midpoints to keep the function continuous between known knots.
     */
    fun calibrate(x: Double): Double {
        val clamped = x.coerceIn(0.0, 1.0)
        if (numBins == 1) return probabilities[0]
        // Find the bin whose midpoint is just above clamped (binary search would be overkill at K~16).
        if (clamped <= binMidpoints[0]) return probabilities[0]
        if (clamped >= binMidpoints[numBins - 1]) return probabilities[numBins - 1]
        var hi = 1
        while (hi < numBins && binMidpoints[hi] < clamped) hi++
        val lo = hi - 1
        val t = (clamped - binMidpoints[lo]) / (binMidpoints[hi] - binMidpoints[lo])
        return probabilities[lo] + t * (probabilities[hi] - probabilities[lo])
    }

    override fun equals(other: Any?): Boolean = other is IsotonicCalibratorResult &&
        numBins == other.numBins && totalWeights == other.totalWeights &&
        binMidpoints.contentEquals(other.binMidpoints) &&
        probabilities.contentEquals(other.probabilities)

    override fun hashCode(): Int {
        var h = numBins
        h = 31 * h + totalWeights.hashCode()
        h = 31 * h + binMidpoints.contentHashCode()
        h = 31 * h + probabilities.contentHashCode()
        return h
    }
}

/**
 * Online isotonic calibration: bins raw scores in `[0, 1]` into [numBins]
 * equal-width buckets, tracks per-bin `(positives, total)` weights, and at
 * read time runs Pool Adjacent Violators to produce a non-decreasing
 * calibrated probability per bin. Unlike [PlattCalibratorStat] this is
 * non-parametric and can absorb arbitrary monotonic miscalibration patterns.
 *
 * Reuses [ReliabilityStat] for the binned `(positives, total)` book-keeping;
 * the only added work is the per-read PAV pass and a linear interpolation in
 * [IsotonicCalibratorResult.calibrate].
 *
 * **Memory:** O([numBins]); three parallel `Double` arrays via
 * [ReliabilityStat].
 *
 * **Update:** O(1) per paired observation.
 *
 * **Concurrency:** Inherits [ReliabilityStat]'s lock-free per-bin atomic adds.
 */
class IsotonicCalibratorStat(
    /** Number of equal-width bins over `[0, 1]`. */
    val numBins: Int = 16,
    override val concurrency: Concurrency = Concurrency.None,
) : PairedStat<IsotonicCalibratorResult> {

    init {
        requirePositiveBins(numBins)
    }

    private val inner = ReliabilityStat(numBins, concurrency)

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) =
        inner.update(x, y, timestampNanos, weight)

    override fun read(timestampNanos: Long): IsotonicCalibratorResult {
        val r = inner.read(timestampNanos)
        // ReliabilityResult already derives this, with the same zero-weight NaN convention.
        val raw = r.outcomeRate
        val weights = r.totalWeights
        val midpoints = DoubleArray(numBins) { (it + 0.5) / numBins }
        val monotone = pav(raw, weights)
        val total = weights.sum()
        return IsotonicCalibratorResult(numBins, midpoints, monotone, total)
    }

    /**
     * Pool Adjacent Violators with weights. Operates left-to-right; whenever a
     * new value violates the running monotone constraint it is merged into the
     * adjacent pool. Empty bins (NaN rate, zero weight) inherit the most
     * recently established pool value so the output stays well-defined.
     */
    private fun pav(values: DoubleArray, weights: DoubleArray): DoubleArray {
        val n = values.size
        // Pools held as parallel arrays; poolStart records the first index each pool
        // covers so the means can be fanned back out at the end.
        val poolMean = DoubleArray(n)
        val poolWeight = DoubleArray(n)
        val poolStart = IntArray(n)
        var pools = 0

        for (i in 0 until n) {
            val w = weights[i]
            if (w <= 0.0) continue
            var mean = values[i]
            var weight = w
            var start = i
            while (pools > 0 && poolMean[pools - 1] >= mean) {
                val pw = poolWeight[pools - 1]
                val pm = poolMean[pools - 1]
                mean = (pm * pw + mean * weight) / (pw + weight)
                weight += pw
                start = poolStart[pools - 1]
                pools--
            }
            poolMean[pools] = mean
            poolWeight[pools] = weight
            poolStart[pools] = start
            pools++
        }

        val out = DoubleArray(n)
        if (pools == 0) return out
        // Fan each pool back across its covered indices.
        for (p in 0 until pools) {
            val from = poolStart[p]
            val to = if (p + 1 < pools) poolStart[p + 1] else n
            for (i in from until to) out[i] = poolMean[p]
        }
        // Indices before the first pool inherit the first pool's value (no data there).
        for (i in 0 until poolStart[0]) out[i] = poolMean[0]
        return out
    }

    override fun merge(values: IsotonicCalibratorResult): Nothing = throw UnsupportedOperationException(
        "IsotonicCalibratorStat does not support merge; merge the underlying ReliabilityStat directly",
    )

    override fun reset() = inner.reset()

    override fun create(concurrency: Concurrency?) = IsotonicCalibratorStat(numBins, concurrency ?: this.concurrency)
}
