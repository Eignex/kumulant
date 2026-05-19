package com.eignex.kumulant.stat.score

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.stat.summary.MeanStat
import com.eignex.kumulant.stat.summary.WeightedMeanResult

/**
 * Streaming pinball / quantile loss at level [tau]. Paired input is
 * `(prediction, truth)`; the per-row loss is
 * `max(tau*(y - yhat), (tau - 1)*(y - yhat))`, which equals `|y - yhat|` when `tau = 0.5`.
 *
 * Used to evaluate quantile regressors (LightGBM/XGBoost quantile objective)
 * and is the single-quantile analog of CRPS.
 *
 * # Concurrency
 *
 * Backed by [MeanStat] — inherits its Welford concurrency model: locked under
 * [Concurrency.Strict] / [Concurrency.HighWrite], lock-free atomic cells with
 * possible ~1e-5 drift under [Concurrency.Relaxed].
 */
class PinballLossStat(
    val tau: Double,
    override val concurrency: Concurrency = Concurrency.None,
) : PairedStat<WeightedMeanResult> {

    init { require(tau in 0.0..1.0) { "tau must be in [0, 1]; got $tau" } }

    private val inner = MeanStat(concurrency)

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        val diff = y - x
        val loss = if (diff >= 0.0) tau * diff else (tau - 1.0) * diff
        inner.update(loss, timestampNanos, weight)
    }

    override fun read(timestampNanos: Long) = inner.read(timestampNanos)
    override fun merge(values: WeightedMeanResult) = inner.merge(values)
    override fun reset() = inner.reset()
    override fun create(concurrency: Concurrency?) = PinballLossStat(tau, concurrency ?: this.concurrency)
}
