package com.eignex.kumulant.bandit

import com.eignex.kumulant.core.Result
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
@Serializable
sealed interface Posterior<R : Result> {
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

/** Beta posterior over a Geometric success probability. */
@Serializable
@SerialName("GeometricBetaPosterior")
data object GeometricBetaPosterior : Posterior<WeightedMeanResult> {
    override fun sample(snapshot: WeightedMeanResult, rng: Random): Double {
        val sum = snapshot.mean * snapshot.totalWeights
        return rng.nextBeta(snapshot.totalWeights, sum - snapshot.totalWeights)
    }
}

/** Gamma posterior over an Exponential rate. */
@Serializable
@SerialName("ExponentialGammaPosterior")
data object ExponentialGammaPosterior : Posterior<WeightedMeanResult> {
    override fun sample(snapshot: WeightedMeanResult, rng: Random): Double {
        val sum = snapshot.mean * snapshot.totalWeights
        return rng.nextGamma(snapshot.totalWeights) / sum
    }
}

/**
 * Normal-Gamma posterior over a normal mean/variance. Draws (variance, mean) jointly:
 * variance ~ Inverse-Gamma(n/2, n·s²/2), mean | variance ~ Normal(snapshot.mean, σ²/n).
 *
 * Re-draws on non-finite intermediates rather than returning NaN. The snapshot must be
 * prior-seeded (n > 0); an unseeded snapshot will spin.
 */
@Serializable
@SerialName("NormalGammaPosterior")
data object NormalGammaPosterior : Posterior<WeightedVarianceResult> {
    override fun sample(snapshot: WeightedVarianceResult, rng: Random): Double {
        while (true) {
            val n = snapshot.totalWeights
            val alpha = n / 2.0
            val beta = snapshot.variance * n / 2.0
            val sampleVariance = beta / rng.nextGamma(alpha)
            if (!sampleVariance.isFinite()) continue
            val value = rng.nextNormal(snapshot.mean, sqrt(sampleVariance / n))
            if (value.isFinite()) return value
        }
    }
}

/**
 * Log-Normal-Gamma posterior: same draw as [NormalGammaPosterior] but exp-transformed
 * back to the real scale. Intended for arms whose stat already accumulates log-rewards
 * (see [LogNormalArm]'s `encode`).
 */
@Serializable
@SerialName("LogNormalGammaPosterior")
data object LogNormalGammaPosterior : Posterior<WeightedVarianceResult> {
    override fun sample(snapshot: WeightedVarianceResult, rng: Random): Double {
        while (true) {
            val n = snapshot.totalWeights
            val alpha = n / 2.0
            val beta = snapshot.variance * n / 2.0
            val sampleVariance = beta / rng.nextGamma(alpha)
            if (!sampleVariance.isFinite()) continue
            val sampleMean = rng.nextNormal(snapshot.mean, sqrt(sampleVariance / n))
            val value = exp(sampleMean + sampleVariance / 2.0)
            if (value.isFinite()) return value
        }
    }
}

/**
 * Gamma posterior over the *scale* of a Gamma likelihood with fixed shape — the shape
 * is a posterior parameter rather than something we infer from data. Not an `object`
 * because of that parameter.
 */
@Serializable
@SerialName("GammaScalePosterior")
data class GammaScalePosterior(val fixedShape: Double) : Posterior<WeightedMeanResult> {
    override fun sample(snapshot: WeightedMeanResult, rng: Random): Double {
        val sum = snapshot.mean * snapshot.totalWeights
        return rng.nextGamma(snapshot.totalWeights * fixedShape) / sum
    }
}
