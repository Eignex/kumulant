package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.stream.additiveMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Sufficient statistics for Beta-Binomial inference: weighted successes and
 * weighted trials. The Beta-Binomial conjugate posterior takes
 * `alpha = successes + alpha_0`, `beta = trials - successes + beta_0`.
 */
@Serializable
@SerialName("BernoulliSumResult")
data class BernoulliSumResult(
    /** Weighted count of success indicators (`Sum w_i * x_i`). */
    val successes: Double,
    /** Cumulative observation weight (`Sum w_i`). */
    val trials: Double,
) : Result

/**
 * Accumulates `(Sum w_i*x_i, Sum w_i)` where each update's value is interpreted as a
 * Bernoulli success indicator (typically 0 or 1; soft probabilities work too).
 *
 * Emits sufficient statistics for binomial / Beta-Binomial inference without
 * performing posterior sampling.
 *
 * **Use cases:** click/conversion accounting, Beta-Binomial bandit arms, A/B
 * sufficient-statistic logging.
 *
 * **Memory:** O(1); two double cells.
 *
 * **Update:** O(1) per observation (two cell adds).
 *
 * **Concurrency:** Two independent additive cells. Concurrent modes preserve
 * per-cell updates; reads can observe successes and trials from different points
 * in the stream. [Concurrency.HighWrite] uses striped adders on the JVM.
 * [Concurrency.None] uses plain cells. Floating-point results depend on update order.
 */
class BernoulliSumStat(override val concurrency: Concurrency = Concurrency.None) : SeriesStat<BernoulliSumResult> {

    private val mode = concurrency.additiveMode()
    private val successes = mode.newDouble(0.0)
    private val trials = mode.newDouble(0.0)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        successes.add(value * weight)
        trials.add(weight)
    }

    override fun read(timestampNanos: Long) = BernoulliSumResult(successes.load(), trials.load())

    override fun merge(values: BernoulliSumResult) {
        successes.add(values.successes)
        trials.add(values.trials)
    }

    override fun reset() {
        successes.store(0.0)
        trials.store(0.0)
    }

    override fun create(concurrency: Concurrency?) = BernoulliSumStat(concurrency ?: this.concurrency)
}
