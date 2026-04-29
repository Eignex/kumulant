package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.HasSampleVariance
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.defaultStreamMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Mean and population variance. */
@Serializable
@SerialName("Variance")
data class VarianceResult(
    val mean: Double,
    val variance: Double
) : Result

/** Weighted mean and variance with [totalWeights] for merge arithmetic. */
@Serializable
@SerialName("WeightedVariance")
data class WeightedVarianceResult(
    override val totalWeights: Double,
    val mean: Double,
    override val variance: Double
) : Result, HasSampleVariance

/**
 * Weighted mean and variance via Welford with Chan-style parallel merge.
 *
 * Population variance `sst / totalWeights`; use [HasSampleVariance.sampleVariance] on
 * the result for the unbiased estimator.
 */
class Variance(
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<WeightedVarianceResult> {

    private val totalWeights = mode.newDouble(0.0)
    private val mean = mode.newDouble(0.0)
    private val sst = mode.newDouble(0.0)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        val oldMean = mean.load()
        val nextW = totalWeights.addAndGet(weight)

        val delta = value - oldMean
        val r = delta * (weight / nextW)

        mean.add(r)
        sst.add((nextW - weight) * delta * r)
    }

    override fun merge(values: WeightedVarianceResult) {
        val w1 = totalWeights.load()
        val w2 = values.totalWeights
        if (w2 <= 0.0) return

        val m1 = mean.load()
        val m2 = values.mean
        val sst2 = values.variance * values.totalWeights

        val nextW = w1 + w2
        val delta = m2 - m1

        val deltaM = delta * (w2 / nextW)
        mean.add(deltaM)

        val sstShift = (delta * delta) * (w1 * w2 / nextW)
        sst.add(sst2 + sstShift)

        totalWeights.add(w2)
    }

    override fun reset() {
        totalWeights.store(0.0)
        mean.store(0.0)
        sst.store(0.0)
    }

    override fun read(timestampNanos: Long): WeightedVarianceResult {
        val totalW = totalWeights.load()
        val variance = if (totalW > 0.0) sst.load() / totalW else 0.0
        return WeightedVarianceResult(totalW, mean.load(), variance)
    }

    override fun create(mode: StreamMode?) = Variance(mode ?: this.mode)
}
