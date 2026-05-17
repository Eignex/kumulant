package com.eignex.kumulant.bandit

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stat.summary.BernoulliSumStat
import com.eignex.kumulant.stat.summary.MeanStat
import com.eignex.kumulant.stat.summary.MomentsStat
import com.eignex.kumulant.stat.summary.VarianceStat
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import com.eignex.kumulant.stat.summary.BernoulliSumResult
import com.eignex.kumulant.stat.summary.MomentsResult
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
    fun createStat(): SeriesStat<R>

    /** Map a raw observation onto the scale the stat accumulates. Identity by default;
     *  [LogNormalArm] overrides with `ln`. */
    fun encode(value: Double): Double = value
}

@Serializable
@SerialName("BernoulliArm")
data class BernoulliArm(
    val priorAlpha: Double = 1.0,
    val priorBeta: Double = 1.0,
) : Arm<BernoulliSumResult> {
    override fun createStat(): SeriesStat<BernoulliSumResult> {
        val s = BernoulliSumStat()
        if (priorAlpha > 0.0) s.update(1.0, 0L, priorAlpha)
        if (priorBeta > 0.0) s.update(0.0, 0L, priorBeta)
        return s
    }
}

/** Single-moment arm (no variance tracking). Used by Poisson, Geometric, Exponential,
 *  GammaScale, which only need `sum = mean · totalWeights`. */
@Serializable
@SerialName("MeanArm")
data class MeanArm(
    val priorMean: Double = 1.0,
    val priorWeight: Double = 0.01,
) : Arm<WeightedMeanResult> {
    override fun createStat(): SeriesStat<WeightedMeanResult> {
        val s = MeanStat()
        if (priorWeight > 0.0) s.update(priorMean, 0L, priorWeight)
        return s
    }
}

@Serializable
@SerialName("NormalArm")
data class NormalArm(
    val priorMean: Double = 0.0,
    val priorWeight: Double = 0.02,
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
}

/** Like [NormalArm] but folds `ln(value)` into the stat. Pair with [LogNormalGammaPosterior]. */
@Serializable
@SerialName("LogNormalArm")
data class LogNormalArm(
    val priorMean: Double = 0.0,
    val priorWeight: Double = 0.02,
    val priorSquaredDeviations: Double = 2.0,
) : Arm<WeightedVarianceResult> {
    override fun createStat(): SeriesStat<WeightedVarianceResult> =
        NormalArm(priorMean, priorWeight, priorSquaredDeviations).createStat()
    override fun encode(value: Double): Double = ln(value)
}

/** Moments-tracking arm used by UCB1Normal / UCB1Tuned (needs `m2`, not just variance). */
@Serializable
@SerialName("MomentsArm")
data class MomentsArm(
    val priorMean: Double = 0.0,
    val priorWeight: Double = 0.02,
) : Arm<MomentsResult> {
    override fun createStat(): SeriesStat<MomentsResult> {
        val s = MomentsStat()
        if (priorWeight > 0.0) s.update(priorMean, 0L, priorWeight)
        return s
    }
}

/** Helper: read `meanOfSquares` from a moments snapshot (= m2/N + mean^2). */
internal fun MomentsResult.meanOfSquares(): Double =
    if (totalWeights > 0.0) m2 / totalWeights + mean * mean else 0.0
