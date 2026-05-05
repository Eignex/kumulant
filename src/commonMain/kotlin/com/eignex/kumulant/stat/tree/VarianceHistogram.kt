package com.eignex.kumulant.stat.tree

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.stat.summary.Variance
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Per-bin `(Σw, mean, variance)` for variance-reduction split-finding in RF
 * regression trees. Variance is the population estimate `sst / Σw`.
 */
@Serializable
@SerialName("VarianceHistogram")
data class VarianceHistogramResult(
    val totalWeights: DoubleArray,
    val mean: DoubleArray,
    val variance: DoubleArray,
) : Result {
    val numBins: Int get() = mean.size

    init {
        require(totalWeights.size == mean.size && mean.size == variance.size) {
            "VarianceHistogramResult arrays must have the same length"
        }
    }

    override fun equals(other: Any?): Boolean = other is VarianceHistogramResult &&
        totalWeights.contentEquals(other.totalWeights) &&
        mean.contentEquals(other.mean) &&
        variance.contentEquals(other.variance)

    override fun hashCode(): Int =
        31 * (31 * totalWeights.contentHashCode() + mean.contentHashCode()) + variance.contentHashCode()
}

/**
 * Per-bin Welford accumulators for RF regression splits. Each bin owns a
 * [Variance] instance, so per-bin numerical stability and merge semantics
 * inherit directly from the underlying Welford recurrence.
 */
class VarianceHistogram(
    val numBins: Int,
    override val concurrency: Concurrency = Concurrency.None,
) : Stat<VarianceHistogramResult> {

    init { require(numBins > 0) { "numBins must be > 0; got $numBins" } }

    private val bins: Array<Variance> = Array(numBins) { Variance(concurrency) }

    fun update(binIndex: Int, value: Double, weight: Double = 1.0) {
        require(binIndex in 0 until numBins) {
            "binIndex $binIndex outside [0, $numBins)"
        }
        bins[binIndex].update(value, weight = weight)
    }

    override fun read(timestampNanos: Long): VarianceHistogramResult {
        val w = DoubleArray(numBins)
        val m = DoubleArray(numBins)
        val v = DoubleArray(numBins)
        for (i in 0 until numBins) {
            val r = bins[i].read(timestampNanos)
            w[i] = r.totalWeights
            m[i] = r.mean
            v[i] = r.variance
        }
        return VarianceHistogramResult(w, m, v)
    }

    override fun merge(values: VarianceHistogramResult) {
        require(values.numBins == numBins) {
            "numBins mismatch on merge: this=$numBins, other=${values.numBins}"
        }
        for (i in 0 until numBins) {
            bins[i].merge(WeightedVarianceResult(values.totalWeights[i], values.mean[i], values.variance[i]))
        }
    }

    override fun reset() {
        for (b in bins) b.reset()
    }

    override fun create(concurrency: Concurrency?) =
        VarianceHistogram(numBins, concurrency ?: this.concurrency)
}
