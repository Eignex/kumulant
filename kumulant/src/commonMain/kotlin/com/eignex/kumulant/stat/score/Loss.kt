package com.eignex.kumulant.stat.score

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.math.PROBABILITY_FLOOR
import com.eignex.kumulant.stat.summary.MeanStat
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import kotlin.math.abs
import kotlin.math.ln

/**
 * Streaming mean squared error: paired `(prediction, truth)` aggregated as the
 * mean of `(prediction - truth)^2`.
 *
 * **Use cases:** regression model evaluation, online MSE tracking on
 * forecasts. Pair with [MaeLossStat] for an L1 view of the same residuals.
 *
 * **Memory:** O(1); backed by a [MeanStat].
 *
 * **Update:** O(1) per paired observation.
 *
 * **Concurrency:** Inherits [MeanStat]'s concurrency model.
 */
class MseLossStat(override val concurrency: Concurrency = Concurrency.None) : PairedStat<WeightedMeanResult> {

    private val inner = MeanStat(concurrency)

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        val diff = x - y
        inner.update(diff * diff, timestampNanos, weight)
    }

    override fun read(timestampNanos: Long) = inner.read(timestampNanos)
    override fun merge(values: WeightedMeanResult, workspace: com.eignex.koblas.Workspace?) = inner.merge(
        values,
        workspace,
    )
    override fun reset() = inner.reset()
    override fun create(concurrency: Concurrency?) = MseLossStat(concurrency ?: this.concurrency)
}

/**
 * Streaming mean absolute error: paired `(prediction, truth)` aggregated as the
 * mean of `|prediction - truth|`.
 *
 * **Use cases:** robust regression-error monitoring (less penalising of
 * outliers than MSE). Pair with [MseLossStat] for both views.
 *
 * **Memory:** O(1); backed by a [MeanStat].
 *
 * **Update:** O(1) per paired observation.
 *
 * **Concurrency:** Inherits [MeanStat]'s concurrency model.
 */
class MaeLossStat(override val concurrency: Concurrency = Concurrency.None) : PairedStat<WeightedMeanResult> {

    private val inner = MeanStat(concurrency)

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        inner.update(abs(x - y), timestampNanos, weight)
    }

    override fun read(timestampNanos: Long) = inner.read(timestampNanos)
    override fun merge(values: WeightedMeanResult, workspace: com.eignex.koblas.Workspace?) = inner.merge(
        values,
        workspace,
    )
    override fun reset() = inner.reset()
    override fun create(concurrency: Concurrency?) = MaeLossStat(concurrency ?: this.concurrency)
}

/**
 * Streaming binary log loss (cross-entropy): paired `(probability, outcome)`
 * aggregated as the mean of `-[y*ln(p) + (1-y)*ln(1-p)]`.
 *
 * Predictions are clamped into `[1e-15, 1 - 1e-15]` before taking logs to avoid
 * `±inf` on perfectly confident wrong predictions.
 *
 * **Use cases:** classifier evaluation; strictly proper scoring rule that
 * heavily penalises confident-and-wrong predictions. Pair with
 * [BrierScoreStat] for the bounded counterpart.
 *
 * **Memory:** O(1); backed by a [MeanStat].
 *
 * **Update:** O(1) per paired observation.
 *
 * **Concurrency:** Inherits [MeanStat]'s concurrency model.
 */
class LogLossStat(override val concurrency: Concurrency = Concurrency.None) : PairedStat<WeightedMeanResult> {

    private val inner = MeanStat(concurrency)

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        val p = x.coerceIn(PROBABILITY_FLOOR, 1.0 - PROBABILITY_FLOOR)
        val loss = -(y * ln(p) + (1.0 - y) * ln(1.0 - p))
        inner.update(loss, timestampNanos, weight)
    }

    override fun read(timestampNanos: Long) = inner.read(timestampNanos)
    override fun merge(values: WeightedMeanResult, workspace: com.eignex.koblas.Workspace?) = inner.merge(
        values,
        workspace,
    )
    override fun reset() = inner.reset()
    override fun create(concurrency: Concurrency?) = LogLossStat(concurrency ?: this.concurrency)
}
