package com.eignex.kumulant.stat.score

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.stat.summary.Mean
import com.eignex.kumulant.stat.summary.WeightedMeanResult

/**
 * Streaming pinball / quantile loss at level [tau]. Paired input is
 * `(prediction, truth)`; the per-row loss is
 * `max(τ·(y − ŷ), (τ − 1)·(y − ŷ))`, which equals `|y − ŷ|` when `τ = 0.5`.
 *
 * Used to evaluate quantile regressors (LightGBM/XGBoost quantile objective)
 * and is the single-quantile analog of CRPS.
 */
class PinballLoss(
    val tau: Double,
    override val concurrency: Concurrency = Concurrency.None,
) : PairedStat<WeightedMeanResult> {

    init { require(tau in 0.0..1.0) { "tau must be in [0, 1]; got $tau" } }

    private val inner = Mean(concurrency)

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        val diff = y - x
        val loss = if (diff >= 0.0) tau * diff else (tau - 1.0) * diff
        inner.update(loss, timestampNanos, weight)
    }

    override fun read(timestampNanos: Long) = inner.read(timestampNanos)
    override fun merge(values: WeightedMeanResult) = inner.merge(values)
    override fun reset() = inner.reset()
    override fun create(concurrency: Concurrency?) = PinballLoss(tau, concurrency ?: this.concurrency)
}
