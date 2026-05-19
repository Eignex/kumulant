package com.eignex.kumulant.stat.score

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.stat.summary.MeanStat
import com.eignex.kumulant.stat.summary.WeightedMeanResult

/**
 * Streaming Brier score for binary probabilistic forecasts. Paired input is
 * `(probability, outcome)` where `outcome  in  {0, 1}`; aggregated as the mean of
 * `(probability - outcome)^2`.
 *
 * Strictly proper scoring rule for binary classification — the binary
 * counterpart to Gaussian CRPS. Lower is better; the bound is `[0, 1]`.
 *
 * **Use cases:** classifier evaluation with a bounded loss — preferred when
 * confident-and-wrong predictions shouldn't be penalised as savagely as
 * [LogLossStat] does. Pair with [ReliabilityStat] for calibration diagnostics.
 *
 * **Memory:** O(1) — backed by a [MeanStat].
 *
 * **Update:** O(1) per paired observation.
 *
 * **Concurrency:** Inherits [MeanStat]'s concurrency model.
 */
class BrierScoreStat(
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
    override fun create(concurrency: Concurrency?) = BrierScoreStat(concurrency ?: this.concurrency)
}
