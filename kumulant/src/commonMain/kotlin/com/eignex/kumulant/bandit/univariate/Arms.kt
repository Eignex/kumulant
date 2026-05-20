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
 * Recipe for one bandit arm's *cumulator side*: how to build a freshly-seeded
 * [SeriesStat] for that arm, and how to encode a raw observation before folding it
 * into the stat. [Posterior]s pair with arm specs of the same [R].
 *
 * The split keeps each concern in one place:
 *   - sufficient-statistic accumulation: kumulant [SeriesStat]
 *   - prior pseudo-counts: this spec's [createStat]
 *   - value transformation (e.g. log-reward): this spec's [encode]
 *   - posterior sampling: a stateless [Posterior]
 *
 * Sealed + `@Serializable` so an arm configuration round-trips on the wire.
 */
@Serializable
sealed interface Arm<R : Result> {
    /** Allocate a fresh per-arm accumulator already seeded with this arm's prior. */
    fun createStat(): SeriesStat<R>

    /** Map a raw observation onto the scale the stat accumulates. Identity by default;
     *  [LogNormalArm] overrides with `ln`. */
    fun encode(value: Double): Double = value
}

/** Bernoulli arm with a Beta(`priorAlpha`, `priorBeta`) prior on the success probability. */
@Serializable
@SerialName("BernoulliArm")
data class BernoulliArm(
    /** Prior pseudo-count of successes. */
    val priorAlpha: Double = 1.0,
    /** Prior pseudo-count of failures. */
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

/** Single-moment arm (no variance tracking). Used by Poisson, Geometric, Exponential,
 *  GammaScale, which only need `sum = mean * totalWeights`. */
@Serializable
@SerialName("MeanArm")
data class MeanArm(
    /** Prior mean reward, seeded as an observation of weight [priorWeight]. */
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

/** Gaussian arm with a Normal-Gamma prior (unknown mean and variance). */
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

/** Like [NormalArm] but folds `ln(value)` into the stat. Pair with [LogNormalGammaPosterior]. */
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

/** Moments-tracking arm used by UCB1Normal / UCB1Tuned (needs `m2`, not just variance). */
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
internal fun MomentsResult.meanOfSquares(): Double =
    if (totalWeights > 0.0) m2 / totalWeights + mean * mean else 0.0

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
fun BernoulliArm.Companion.warmStart(
    global: BernoulliSumResult,
    shrinkage: Double = 1.0,
): BernoulliArm {
    require(shrinkage in 0.0..1.0) { "shrinkage must be in [0, 1], got $shrinkage" }
    return BernoulliArm(
        priorAlpha = global.successes * shrinkage,
        priorBeta = (global.trials - global.successes) * shrinkage,
    )
}

/** Warm-started [MeanArm] from a global weighted-mean snapshot. */
fun MeanArm.Companion.warmStart(
    global: WeightedMeanResult,
    shrinkage: Double = 1.0,
): MeanArm {
    require(shrinkage in 0.0..1.0) { "shrinkage must be in [0, 1], got $shrinkage" }
    return MeanArm(
        priorMean = global.mean,
        priorWeight = global.totalWeights * shrinkage,
    )
}

/** Warm-started [NormalArm] from a global weighted-variance snapshot. The arm's prior
 *  variance is preserved from the global; only the prior weight is shrunk. */
fun NormalArm.Companion.warmStart(
    global: WeightedVarianceResult,
    shrinkage: Double = 1.0,
): NormalArm {
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
fun LogNormalArm.Companion.warmStart(
    global: WeightedVarianceResult,
    shrinkage: Double = 1.0,
): LogNormalArm {
    require(shrinkage in 0.0..1.0) { "shrinkage must be in [0, 1], got $shrinkage" }
    val priorWeight = global.totalWeights * shrinkage
    return LogNormalArm(
        priorMean = global.mean,
        priorWeight = priorWeight,
        priorSquaredDeviations = global.variance * priorWeight,
    )
}

/** Warm-started [MomentsArm] from a global moments snapshot. */
fun MomentsArm.Companion.warmStart(
    global: MomentsResult,
    shrinkage: Double = 1.0,
): MomentsArm {
    require(shrinkage in 0.0..1.0) { "shrinkage must be in [0, 1], got $shrinkage" }
    return MomentsArm(
        priorMean = global.mean,
        priorWeight = global.totalWeights * shrinkage,
    )
}
