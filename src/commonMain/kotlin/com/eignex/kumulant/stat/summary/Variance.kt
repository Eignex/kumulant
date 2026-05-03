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
    override val mode: StreamMode = defaultStreamMode,
) : SeriesStat<WeightedVarianceResult> {

    private val totalWeights = mode.newDouble(0.0)
    private val mean = mode.newDouble(0.0)
    private val sst = mode.newDouble(0.0)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight == 0.0) return
        val priorW = totalWeights.load()
        val nextW = totalWeights.addAndGet(weight)
        val delta = value - mean.load()
        val r = delta * (weight / nextW)
        mean.add(r)
        sst.add(priorW * delta * r)
    }

    override fun merge(values: WeightedVarianceResult) {
        if (values.totalWeights <= 0.0) return
        val w1 = totalWeights.load()
        val w2 = values.totalWeights
        val nextW = totalWeights.addAndGet(w2)
        val delta = values.mean - mean.load()
        mean.add(delta * (w2 / nextW))
        sst.add(values.variance * w2 + (delta * delta) * (w1 * w2 / nextW))
    }

    override fun reset() {
        totalWeights.store(0.0)
        mean.store(0.0)
        sst.store(0.0)
    }

    override fun read(timestampNanos: Long): WeightedVarianceResult {
        val w = totalWeights.load()
        val m = mean.load()
        val s = sst.load()
        val variance = if (w > 0.0) s / w else 0.0
        return WeightedVarianceResult(w, m, variance)
    }

    override fun create(mode: StreamMode?) = Variance(mode ?: this.mode)
}
