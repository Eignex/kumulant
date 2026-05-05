package com.eignex.kumulant.stat.score

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.core.defaultConcurrency
import com.eignex.kumulant.forecast.GaussianForecast
import com.eignex.kumulant.stat.summary.Mean
import com.eignex.kumulant.stat.summary.WeightedMeanResult

/**
 * Streaming mean CRPS for Gaussian forecasts. Each update is a 3-vector
 * `[mean, stdDev, y]`; the per-row score is `GaussianForecast(mean, stdDev).crps(y)`,
 * aggregated as a [Mean]. Use to evaluate a probabilistic forecaster against truth.
 */
class CrpsGaussian(
    override val concurrency: Concurrency = defaultConcurrency,
) : VectorStat<WeightedMeanResult> {

    private val inner = Mean(concurrency)

    override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) {
        require(vector.size == 3) {
            "CrpsGaussian expects [mean, stdDev, y]; got size ${vector.size}"
        }
        val crps = GaussianForecast(vector[0], vector[1]).crps(vector[2])
        inner.update(crps, timestampNanos, weight)
    }

    override fun read(timestampNanos: Long): WeightedMeanResult = inner.read(timestampNanos)
    override fun merge(values: WeightedMeanResult) = inner.merge(values)
    override fun reset() = inner.reset()
    override fun create(concurrency: Concurrency?): CrpsGaussian =
        CrpsGaussian(concurrency ?: this.concurrency)
}
