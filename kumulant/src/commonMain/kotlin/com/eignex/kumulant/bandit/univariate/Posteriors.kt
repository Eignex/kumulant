package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.math.nextBeta
import com.eignex.kumulant.math.nextGamma
import com.eignex.kumulant.math.nextNormal
import com.eignex.kumulant.schema.serialization.PosteriorSerializer
import com.eignex.kumulant.stat.summary.BernoulliSumResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.exp
import kotlin.math.sqrt
import kotlin.random.Random

/**
 * Stateless conjugate posterior over a univariate likelihood, parameterised by the
 * sufficient-statistic snapshot [R]. A [Posterior] is a pure `(snapshot, rng) -> sample`
 * function: no priors, no per-arm state, no update path. Arm lifecycle, value encoding,
 * and prior seeding all live in [Arm].
 *
 * Sealed + `@Serializable` so a `(arm, posterior)` Thompson configuration is wire-portable
 * via the kumulant/skema serialization convention. The posterior assumes the snapshot
 * already incorporates whatever pseudo-counts the caller wants; with a seeded snapshot
 * the empty-snapshot edge case can't arise.
 */
@Serializable(with = PosteriorSerializer::class)
sealed interface Posterior<R : Result> {
    /** Draw a single Monte Carlo sample of the arm's reward from its posterior. */
    fun sample(snapshot: R, rng: Random): Double
}

/** Beta posterior over a Bernoulli rate. `successes` and `trials-successes` are the
 *  Beta parameters; both must be positive (i.e. snapshot must be prior-seeded). */
@Serializable
@SerialName("BetaPosterior")
data object BetaPosterior : Posterior<BernoulliSumResult> {
    override fun sample(snapshot: BernoulliSumResult, rng: Random): Double {
        val alpha = snapshot.successes
        val beta = snapshot.trials - snapshot.successes
        return rng.nextBeta(alpha, beta)
    }
}

/** Gamma posterior over a Poisson rate: `Gamma(sum, totalWeights)`. */
@Serializable
@SerialName("PoissonGammaPosterior")
data object PoissonGammaPosterior : Posterior<WeightedMeanResult> {
    override fun sample(snapshot: WeightedMeanResult, rng: Random): Double {
        val sum = snapshot.mean * snapshot.totalWeights
        return rng.nextGamma(sum) / snapshot.totalWeights
    }
}

/** Gamma posterior over an Exponential rate, reported as the mean inter-arrival time `1/lambda`. */
@Serializable
@SerialName("ExponentialGammaPosterior")
data object ExponentialGammaPosterior : Posterior<WeightedMeanResult> {
    override fun sample(snapshot: WeightedMeanResult, rng: Random): Double {
        val sum = snapshot.mean * snapshot.totalWeights
        return sum / rng.nextGamma(snapshot.totalWeights)
    }
}

// A finite, positive-weight snapshot draws a finite value almost surely, so the cap only bounds a
// snapshot whose parameters overflow the sampler.
private const val MAX_POSTERIOR_REDRAWS = 64

private fun WeightedVarianceResult.isSampleable(): Boolean =
    totalWeights > 0.0 && totalWeights.isFinite() && mean.isFinite() && variance.isFinite()

// Draws (variance, mean) from the Normal-Gamma posterior and maps the draw through [toReward],
// re-drawing on non-finite intermediates a bounded number of times before giving up with NaN.
private inline fun WeightedVarianceResult.drawNormalGamma(
    rng: Random,
    toReward: (mean: Double, variance: Double) -> Double,
): Double {
    if (!isSampleable()) return Double.NaN
    repeat(MAX_POSTERIOR_REDRAWS) {
        val n = totalWeights
        val sampleVariance = (variance * n / 2.0) / rng.nextGamma(n / 2.0)
        if (!sampleVariance.isFinite()) return@repeat
        val value = toReward(rng.nextNormal(mean, sqrt(sampleVariance / n)), sampleVariance)
        if (value.isFinite()) return value
    }
    return Double.NaN
}

/**
 * Normal-Gamma posterior over a normal mean/variance. Draws (variance, mean) jointly:
 * variance ~ Inverse-Gamma(n/2, n*s^2/2), mean | variance ~ Normal(snapshot.mean, sigma^2/n).
 *
 * Re-draws on non-finite intermediates a bounded number of times. A snapshot with no usable
 * evidence (non-positive or non-finite weight, non-finite mean or variance, as left by a `NaN` or
 * infinite reward) scores `NaN`, which loses every arm comparison instead of spinning.
 */
@Serializable
@SerialName("NormalGammaPosterior")
data object NormalGammaPosterior : Posterior<WeightedVarianceResult> {
    override fun sample(snapshot: WeightedVarianceResult, rng: Random): Double =
        snapshot.drawNormalGamma(rng) { mean, _ -> mean }
}

/**
 * Log-Normal-Gamma posterior: same draw as [NormalGammaPosterior] but exp-transformed
 * back to the real scale. Intended for arms whose stat already accumulates log-rewards
 * (see [LogNormalArm]'s `encode`).
 */
@Serializable
@SerialName("LogNormalGammaPosterior")
data object LogNormalGammaPosterior : Posterior<WeightedVarianceResult> {
    override fun sample(snapshot: WeightedVarianceResult, rng: Random): Double =
        snapshot.drawNormalGamma(rng) { mean, variance -> exp(mean + variance / 2.0) }
}
