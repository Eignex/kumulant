package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasSampleVariance
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.welfordLock
import com.eignex.kumulant.stream.welfordMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** MeanStat and population variance. */
@Serializable
@SerialName("VarianceStat")
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
class VarianceStat(
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<WeightedVarianceResult> {

    private val mode = concurrency.welfordMode()
    private val lock = concurrency.welfordLock()
    private val totalWeights = mode.newDouble(0.0)
    private val mean = mode.newDouble(0.0)
    private val sst = mode.newDouble(0.0)

    override fun update(value: Double, timestampNanos: Long, weight: Double) = lock.withLock {
        if (weight == 0.0) return@withLock
        val priorW = totalWeights.load()
        val nextW = totalWeights.addAndGet(weight)
        val delta = value - mean.load()
        val r = delta * (weight / nextW)
        mean.add(r)
        sst.add(priorW * delta * r)
    }

    override fun merge(values: WeightedVarianceResult) = lock.withLock {
        if (values.totalWeights <= 0.0) return@withLock
        val w1 = totalWeights.load()
        val w2 = values.totalWeights
        val nextW = totalWeights.addAndGet(w2)
        val delta = values.mean - mean.load()
        mean.add(delta * (w2 / nextW))
        sst.add(values.variance * w2 + (delta * delta) * (w1 * w2 / nextW))
    }

    override fun reset() = lock.withLock {
        totalWeights.store(0.0)
        mean.store(0.0)
        sst.store(0.0)
    }

    override fun read(timestampNanos: Long): WeightedVarianceResult = lock.withLock {
        val w = totalWeights.load()
        val m = mean.load()
        val s = sst.load()
        val variance = if (w > 0.0) s / w else 0.0
        WeightedVarianceResult(w, m, variance)
    }

    override fun create(concurrency: Concurrency?) = VarianceStat(concurrency ?: this.concurrency)
}
