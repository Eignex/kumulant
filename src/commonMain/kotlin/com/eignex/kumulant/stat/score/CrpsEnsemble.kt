package com.eignex.kumulant.stat.score

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.forecast.EnsembleForecast
import com.eignex.kumulant.stat.summary.Mean
import com.eignex.kumulant.stat.summary.WeightedMeanResult

/**
 * Streaming mean CRPS for ensemble forecasts. The (forecast, observation)
 * input shape doesn't fit any modality (ensemble size varies per row), so
 * this is a raw [Stat] with a typed update method — same pattern as the tree
 * histograms.
 *
 * Per-update cost is dominated by [EnsembleForecast.crps] (`O(m)` after the
 * one-time sort done at forecast construction). Aggregation itself is a
 * [Mean] and adds negligible overhead.
 */
class CrpsEnsemble(
    override val concurrency: Concurrency = Concurrency.None,
) : Stat<WeightedMeanResult> {

    private val inner = Mean(concurrency)

    fun update(forecast: EnsembleForecast, y: Double, weight: Double = 1.0) {
        inner.update(forecast.crps(y), weight = weight)
    }

    override fun read(timestampNanos: Long) = inner.read(timestampNanos)
    override fun merge(values: WeightedMeanResult) = inner.merge(values)
    override fun reset() = inner.reset()
    override fun create(concurrency: Concurrency?) = CrpsEnsemble(concurrency ?: this.concurrency)
}
