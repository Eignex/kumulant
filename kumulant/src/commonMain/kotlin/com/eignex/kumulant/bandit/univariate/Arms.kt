package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stat.summary.BernoulliSumResult
import com.eignex.kumulant.stat.summary.BernoulliSumStat
import com.eignex.kumulant.stat.summary.MeanStat
import com.eignex.kumulant.stat.summary.MomentsResult
import com.eignex.kumulant.stat.summary.MomentsStat
import com.eignex.kumulant.stat.summary.VarianceStat
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.ln
import kotlin.math.sqrt

/**
 * Recipe for one bandit arm's cumulator side: how to build a freshly-seeded
 * [SeriesStat] for that arm, and how to encode a raw observation before
 * folding it into the stat. [Posterior]s and [BanditPolicy]s pair with arm
 * specs of the same [R].
 *
 * The split keeps each concern in one place:
 *
 * - **Sufficient-statistic accumulation**; kumulant's [SeriesStat] families.
 * - **Prior pseudo-counts**; this spec's [createStat] seeds the accumulator
 *   so a posterior or UCB formula evaluated immediately at empty returns a
 *   well-defined finite score.
 * - **Value transformation**; this spec's [encode] maps a raw observation
 *   onto the scale the stat accumulates. Identity for most arms;
 *   [LogNormalArm] overrides with `ln` so multiplicative rewards are
 *   accumulated on a log scale.
 * - **Posterior sampling**; stateless [Posterior]; consumes the same
 *   snapshot the stat produces.
 *
 * Sealed + `@Serializable` so an arm configuration round-trips on the wire
 * alongside the rest of the [UnivariateBanditSpec].
 *
 * ## Picking an arm
 *
 * - [BernoulliArm]; binary reward; result is [BernoulliSumResult]; pairs
 *   with [BetaPosterior] (Thompson) or [UCB1].
 * - [MeanArm]; single-moment likelihoods (Poisson, Geometric, Exponential,
 *   GammaScale); result is [WeightedMeanResult]; pairs with one of the
 *   single-moment posteriors ([PoissonGammaPosterior],
 *   [GeometricBetaPosterior], [ExponentialGammaPosterior],
 *   [GammaScalePosterior]).
 * - [NormalArm]; Gaussian reward with unknown mean and variance; result is
 *   [WeightedVarianceResult]; pairs with [NormalGammaPosterior] (Thompson)
 *   or the mean-based policies ([Greedy], [EpsilonGreedy],
 *   [EpsilonDecreasing], [UniformSelection]).
 * - [LogNormalArm]; multiplicative reward (revenue, latency); result is
 *   [WeightedVarianceResult] on the log scale; pairs with
 *   [LogNormalGammaPosterior].
 * - [MomentsArm]; Gaussian with second-moment tracking; result is
 *   [MomentsResult]; pairs with the variance-aware UCB family
 *   ([UCB1Normal], [UCB1Tuned], [UcbV]).
 */
@Serializable
sealed interface Arm<R : Result> {
    /**
     * Allocate a fresh per-arm accumulator already seeded with this arm's
     * prior pseudo-counts.
     */
    fun createStat(): SeriesStat<R>

    /**
     * Map a raw observation onto the scale the stat accumulates. Identity
     * by default; [LogNormalArm] overrides with `ln` so the underlying
     * stat tracks the log-reward and the Normal-Gamma posterior fits the
     * log-normal generative model.
     */
    fun encode(value: Double): Double = value
}

/**
 * Bernoulli arm. The reward is binary `{0, 1}` and the unknown is the
 * success probability `p`. A Beta(`priorAlpha`, `priorBeta`) prior is
 * conjugate to the Bernoulli likelihood; the posterior is
 * `Beta(priorAlpha + successes, priorBeta + failures)`.
 *
 * Default `Beta(1, 1)` is the uniform prior on `p`; neutral, the standard
 * "I know nothing" starting point. `Beta(1, 9)` is mildly pessimistic
 * (expecting failures), `Beta(9, 1)` mildly optimistic.
 *
 * Pair with [BetaPosterior] for Thompson sampling or [UCB1] for the
 * confidence-bound family.
 */
@Serializable
@SerialName("BernoulliArm")
data class BernoulliArm(
    /** Prior pseudo-count of successes. Higher = stronger belief that the arm is good. */
    val priorAlpha: Double = 1.0,
    /** Prior pseudo-count of failures. Higher = stronger belief that the arm is bad. */
    val priorBeta: Double = 1.0,
) : Arm<BernoulliSumResult> {
    override fun createStat(): SeriesStat<BernoulliSumResult> {
        val s = BernoulliSumStat()
        if (priorAlpha > 0.0) s.update(1.0, 0L, priorAlpha)
        if (priorBeta > 0.0) s.update(0.0, 0L, priorBeta)
        return s
    }

    /** Factory entry-point for [BernoulliArm] (host for [warmStart]). */
    companion object
}

/**
 * Single-moment arm; tracks the running mean but not variance. The right
 * pick when the likelihood's sufficient statistic is one running sum (or
 * equivalently a running mean × count):
 *
 * - **Poisson reward** (count data); pair with [PoissonGammaPosterior].
 * - **Geometric reward** (trials until success); pair with
 *   [GeometricBetaPosterior].
 * - **Exponential reward** (inter-arrival time); pair with
 *   [ExponentialGammaPosterior].
 * - **Gamma reward with known shape**; pair with [GammaScalePosterior].
 *
 * The prior is a pseudo-observation of value [priorMean] with weight
 * [priorWeight]. Tiny `priorWeight` (default 0.01) makes the prior a soft
 * suggestion that gets washed out in the first few real observations.
 */
@Serializable
@SerialName("MeanArm")
data class MeanArm(
    /** Prior mean reward, seeded as a pseudo-observation of weight [priorWeight]. */
    val priorMean: Double = 1.0,
    /** Pseudo-weight of the prior seed; smaller = weaker prior. */
    val priorWeight: Double = 0.01,
) : Arm<WeightedMeanResult> {
    override fun createStat(): SeriesStat<WeightedMeanResult> {
        val s = MeanStat()
        if (priorWeight > 0.0) s.update(priorMean, 0L, priorWeight)
        return s
    }

    /** Factory entry-point for [MeanArm] (host for [warmStart]). */
    companion object
}

/**
 * Gaussian arm with a Normal-Gamma prior (unknown mean and variance).
 * Tracks both the running mean and the sum of squared deviations, which
 * gives [NormalGammaPosterior] enough to draw `(mean, variance)` jointly
 * and gives the variance-aware policies ([Greedy], [EpsilonGreedy]) a
 * reasonable variance estimate.
 *
 * The prior is parameterised as a Normal-Gamma:
 *
 * - [priorMean] is the prior mean reward.
 * - [priorWeight] is the pseudo-weight of the prior; how many "phantom
 *   observations" the prior counts as. Higher = stronger prior, slower to
 *   move.
 * - [priorSquaredDeviations] is the prior sum of squared deviations;
 *   tightens the prior on the variance. Higher = stronger belief that
 *   variance is large.
 *
 * The default prior is mildly informative; `priorWeight = 0.02` washes
 * out after a few observations.
 *
 * For multiplicative rewards (revenue, latency, anything where noise is
 * log-normal rather than additive Gaussian), use [LogNormalArm] instead.
 */
@Serializable
@SerialName("NormalArm")
data class NormalArm(
    /** Prior mean reward. */
    val priorMean: Double = 0.0,
    /** Pseudo-weight of the prior seed. */
    val priorWeight: Double = 0.02,
    /** Prior sum of squared deviations; tightens the prior on the variance. */
    val priorSquaredDeviations: Double = 0.02,
) : Arm<WeightedVarianceResult> {
    override fun createStat(): SeriesStat<WeightedVarianceResult> {
        val s = VarianceStat()
        if (priorWeight > 0.0) {
            s.update(priorMean, 0L, priorWeight)
            if (priorSquaredDeviations > 0.0) {
                val sigma = sqrt(priorSquaredDeviations / priorWeight)
                s.update(priorMean + sigma, 0L, priorWeight / 2.0)
                s.update(priorMean - sigma, 0L, priorWeight / 2.0)
            }
        }
        return s
    }

    /** Factory entry-point for [NormalArm] (host for [warmStart]). */
    companion object
}

/**
 * Like [NormalArm] but folds `ln(value)` into the stat via [encode]. The
 * right pick when rewards are multiplicative rather than additive; revenue
 * per session, latency in milliseconds, anything where the noise scales
 * with the magnitude.
 *
 * The Normal-Gamma posterior on the log scale corresponds to a log-normal
 * generative model: `log(reward) ~ Normal(mu, sigma^2)`. The default prior
 * is broader than [NormalArm]'s (`priorSquaredDeviations = 2.0` vs `0.02`)
 * because log-scale rewards typically have larger variance per arm than
 * the linear-scale equivalent.
 *
 * Pair with [LogNormalGammaPosterior], which transforms the sampled log-
 * scale mean back to the original scale via `exp(mean + variance / 2)`.
 *
 * **Caveat:** raw rewards must be strictly positive; `ln(0)` is `-inf`
 * and `ln(negative)` is `NaN`. Pre-filter or clamp non-positive observations
 * before feeding them to the bandit.
 */
@Serializable
@SerialName("LogNormalArm")
data class LogNormalArm(
    /** Prior mean of `ln(reward)`. */
    val priorMean: Double = 0.0,
    /** Pseudo-weight of the prior seed. */
    val priorWeight: Double = 0.02,
    /** Prior sum of squared deviations on the log scale. */
    val priorSquaredDeviations: Double = 2.0,
) : Arm<WeightedVarianceResult> {
    override fun createStat(): SeriesStat<WeightedVarianceResult> =
        NormalArm(priorMean, priorWeight, priorSquaredDeviations).createStat()
    override fun encode(value: Double): Double = ln(value)

    /** Factory entry-point for [LogNormalArm] (host for [warmStart]). */
    companion object
}

/**
 * Moments-tracking arm. Backs [MomentsStat] under the hood, which means the
 * snapshot exposes the raw second moment `m2` (in addition to mean and
 * variance); required by the variance-aware UCB policies that need
 * `mean-of-squares` directly:
 *
 * - [UCB1Normal]; Auer et al.'s variance-aware UCB for Gaussian rewards.
 * - [UCB1Tuned]; sample-variance-aware UCB; typically tighter bound than
 *   plain [UCB1].
 * - [UcbV]; variance-aware UCB with an explicit exploration constant.
 *
 * For pure Thompson sampling on Gaussian rewards, [NormalArm] + the
 * Normal-Gamma posterior is the right pick; [MomentsArm] earns its place
 * specifically when the policy needs `m2`.
 */
@Serializable
@SerialName("MomentsArm")
data class MomentsArm(
    /** Prior mean reward. */
    val priorMean: Double = 0.0,
    /** Pseudo-weight of the prior seed. */
    val priorWeight: Double = 0.02,
) : Arm<MomentsResult> {
    override fun createStat(): SeriesStat<MomentsResult> {
        val s = MomentsStat()
        if (priorWeight > 0.0) s.update(priorMean, 0L, priorWeight)
        return s
    }

    /** Factory entry-point for [MomentsArm] (host for [warmStart]). */
    companion object
}

/** Helper: read `meanOfSquares` from a moments snapshot (= m2/N + mean^2). */
internal fun MomentsResult.meanOfSquares(): Double = if (totalWeights > 0.0) m2 / totalWeights + mean * mean else 0.0

/**
 * Warm-started [BernoulliArm] from a global Bernoulli snapshot.
 *
 * Builds an Arm spec from a pooled global snapshot. [shrinkage] in `[0, 1]`
 * scales how much of the global evidence is counted as the arm's prior
 * pseudo-count: `0` collapses to the bare prior; `1` treats every global
 * observation as if it had been seen by this arm. The same convention applies
 * to every `warmStart` overload below.
 *
 * Univariate hierarchical pooling has no clean general API (see the discussion
 * next to [com.eignex.kumulant.bandit.contextual.RegressionContextualBandit] for the
 * contextual case); these per-arm-type helpers cover the Result types where
 * shrinkage has a uniform pseudo-count interpretation.
 */
fun BernoulliArm.Companion.warmStart(global: BernoulliSumResult, shrinkage: Double = 1.0): BernoulliArm {
    require(shrinkage in 0.0..1.0) { "shrinkage must be in [0, 1], got $shrinkage" }
    return BernoulliArm(
        priorAlpha = global.successes * shrinkage,
        priorBeta = (global.trials - global.successes) * shrinkage,
    )
}

/** Warm-started [MeanArm] from a global weighted-mean snapshot. */
fun MeanArm.Companion.warmStart(global: WeightedMeanResult, shrinkage: Double = 1.0): MeanArm {
    require(shrinkage in 0.0..1.0) { "shrinkage must be in [0, 1], got $shrinkage" }
    return MeanArm(
        priorMean = global.mean,
        priorWeight = global.totalWeights * shrinkage,
    )
}

/** Warm-started [NormalArm] from a global weighted-variance snapshot. The arm's prior
 *  variance is preserved from the global; only the prior weight is shrunk. */
fun NormalArm.Companion.warmStart(global: WeightedVarianceResult, shrinkage: Double = 1.0): NormalArm {
    require(shrinkage in 0.0..1.0) { "shrinkage must be in [0, 1], got $shrinkage" }
    val priorWeight = global.totalWeights * shrinkage
    return NormalArm(
        priorMean = global.mean,
        priorWeight = priorWeight,
        priorSquaredDeviations = global.variance * priorWeight,
    )
}

/** Warm-started [LogNormalArm] from a global weighted-variance snapshot on the log
 *  scale (caller is responsible for ensuring the snapshot is over `ln(reward)`). */
fun LogNormalArm.Companion.warmStart(global: WeightedVarianceResult, shrinkage: Double = 1.0): LogNormalArm {
    require(shrinkage in 0.0..1.0) { "shrinkage must be in [0, 1], got $shrinkage" }
    val priorWeight = global.totalWeights * shrinkage
    return LogNormalArm(
        priorMean = global.mean,
        priorWeight = priorWeight,
        priorSquaredDeviations = global.variance * priorWeight,
    )
}

/** Warm-started [MomentsArm] from a global moments snapshot. */
fun MomentsArm.Companion.warmStart(global: MomentsResult, shrinkage: Double = 1.0): MomentsArm {
    require(shrinkage in 0.0..1.0) { "shrinkage must be in [0, 1], got $shrinkage" }
    return MomentsArm(
        priorMean = global.mean,
        priorWeight = global.totalWeights * shrinkage,
    )
}
