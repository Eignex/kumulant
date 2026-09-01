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
 * Mirrors combo's `BinarySum`: emits the sufficient statistics for binomial /
 * Beta-Binomial Thompson sampling without doing the sampling itself.
 *
 * **Use cases:** click/conversion accounting, Beta-Binomial bandit arms, A/B
 * sufficient-statistic logging.
 *
 * **Memory:** O(1); two double cells.
 *
 * **Update:** O(1) per observation (two atomic adds).
 *
 * **Concurrency:** Two independent atomic adds per update; exact under every
 * [Concurrency] level. A `read()` interleaved between the two writes of one
 * update can briefly observe successes/trials mismatched by one observation,
 * but the per-cell guarantees hold. [Concurrency.HighWrite] switches both
 * cells to striped adders.
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

    override fun merge(values: BernoulliSumResult, workspace: com.eignex.koblas.Workspace?) {
        successes.add(values.successes)
        trials.add(values.trials)
    }

    override fun reset() {
        successes.store(0.0)
        trials.store(0.0)
    }

    override fun create(concurrency: Concurrency?) = BernoulliSumStat(concurrency ?: this.concurrency)
}
