package com.eignex.kumulant.stat.score

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.stat.summary.Mean
import com.eignex.kumulant.stat.summary.WeightedMeanResult

/**
 * Streaming Brier score for binary probabilistic forecasts. Paired input is
 * `(probability, outcome)` where `outcome ∈ {0, 1}`; aggregated as the mean of
 * `(probability − outcome)²`.
 *
 * Strictly proper scoring rule for binary classification — the binary
 * counterpart to Gaussian CRPS. Lower is better; the bound is `[0, 1]`.
 */
class BrierScore(
    override val concurrency: Concurrency = Concurrency.None,
) : PairedStat<WeightedMeanResult> {

    private val inner = Mean(concurrency)

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        val diff = x - y
        inner.update(diff * diff, timestampNanos, weight)
    }

    override fun read(timestampNanos: Long) = inner.read(timestampNanos)
    override fun merge(values: WeightedMeanResult) = inner.merge(values)
    override fun reset() = inner.reset()
    override fun create(concurrency: Concurrency?) = BrierScore(concurrency ?: this.concurrency)
}
