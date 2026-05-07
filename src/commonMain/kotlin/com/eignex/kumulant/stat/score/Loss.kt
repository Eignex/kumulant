package com.eignex.kumulant.stat.score

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.stat.summary.MeanStat
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import kotlin.math.abs
import kotlin.math.ln

private const val LOG_LOSS_EPS: Double = 1e-15

/**
 * Streaming mean squared error: paired `(prediction, truth)` aggregated as the
 * mean of `(prediction − truth)²`.
 */
class MseLossStat(
    override val concurrency: Concurrency = Concurrency.None,
) : PairedStat<WeightedMeanResult> {

    private val inner = MeanStat(concurrency)

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        val diff = x - y
        inner.update(diff * diff, timestampNanos, weight)
    }

    override fun read(timestampNanos: Long) = inner.read(timestampNanos)
    override fun merge(values: WeightedMeanResult) = inner.merge(values)
    override fun reset() = inner.reset()
    override fun create(concurrency: Concurrency?) = MseLossStat(concurrency ?: this.concurrency)
}

/**
 * Streaming mean absolute error: paired `(prediction, truth)` aggregated as the
 * mean of `|prediction − truth|`.
 */
class MaeLossStat(
    override val concurrency: Concurrency = Concurrency.None,
) : PairedStat<WeightedMeanResult> {

    private val inner = MeanStat(concurrency)

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        inner.update(abs(x - y), timestampNanos, weight)
    }

    override fun read(timestampNanos: Long) = inner.read(timestampNanos)
    override fun merge(values: WeightedMeanResult) = inner.merge(values)
    override fun reset() = inner.reset()
    override fun create(concurrency: Concurrency?) = MaeLossStat(concurrency ?: this.concurrency)
}

/**
 * Streaming binary log loss (cross-entropy): paired `(probability, outcome)`
 * aggregated as the mean of `−[y·ln(p) + (1−y)·ln(1−p)]`.
 *
 * Predictions are clamped into `[1e-15, 1 − 1e-15]` before taking logs to avoid
 * `±∞` on perfectly confident wrong predictions.
 */
class LogLossStat(
    override val concurrency: Concurrency = Concurrency.None,
) : PairedStat<WeightedMeanResult> {

    private val inner = MeanStat(concurrency)

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        val p = x.coerceIn(LOG_LOSS_EPS, 1.0 - LOG_LOSS_EPS)
        val loss = -(y * ln(p) + (1.0 - y) * ln(1.0 - p))
        inner.update(loss, timestampNanos, weight)
    }

    override fun read(timestampNanos: Long) = inner.read(timestampNanos)
    override fun merge(values: WeightedMeanResult) = inner.merge(values)
    override fun reset() = inner.reset()
    override fun create(concurrency: Concurrency?) = LogLossStat(concurrency ?: this.concurrency)
}
