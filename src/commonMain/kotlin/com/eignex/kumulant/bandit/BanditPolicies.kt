package com.eignex.kumulant.bandit

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stat.summary.BernoulliSumResult
import com.eignex.kumulant.stat.summary.MomentsResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.math.ln
import kotlin.math.min
import kotlin.math.pow
import kotlin.math.sqrt
import kotlin.random.Random

/**
 * Decides which arm to play given snapshots of each arm's sufficient statistic [R].
 *
 * The policy owns the per-arm cumulator lifecycle through its [arm] spec:
 *   - [createArm] returns a freshly-prior-seeded [SeriesStat] from `arm.createStat()`;
 *   - [update] folds an observation in, applying `arm.encode` first.
 *
 * Sampling-based policies additionally carry a stateless [Posterior]; UCB-style
 * policies compute their score from the snapshot directly. Per-policy global state
 * (e.g. total samples for UCB) is exposed via [addArm]/[removeArm].
 */
interface BanditPolicy<R : Result> {
    val arm: Arm<R>

    fun createArm(): SeriesStat<R> = arm.createStat()
    fun update(stat: SeriesStat<R>, value: Double, weight: Double = 1.0) {
        stat.update(arm.encode(value), 0L, weight)
    }
    fun evaluate(snapshot: R, step: Long, maximize: Boolean, rng: Random): Double

    fun addArm(snapshot: R) {}
    fun removeArm(snapshot: R) {}
}

private fun signedMean(mean: Double, maximize: Boolean) = if (maximize) mean else -mean

/**
 * Thompson sampling: score each arm by a draw from its [posterior] given the snapshot.
 * Pair an [arm] spec with a posterior of the same [R].
 */
class ThompsonSampling<R : Result>(
    override val arm: Arm<R>,
    val posterior: Posterior<R>,
) : BanditPolicy<R> {
    override fun evaluate(snapshot: R, step: Long, maximize: Boolean, rng: Random) =
        signedMean(posterior.sample(snapshot, rng), maximize)
}

// === Canonical pairings: arm + matching posterior ==========================
// PascalCase below is the convention for constructor-shaped factories.

@Suppress("FunctionNaming")
fun BetaBernoulliTS(priorAlpha: Double = 1.0, priorBeta: Double = 1.0) =
    ThompsonSampling(BernoulliArm(priorAlpha, priorBeta), BetaPosterior)

@Suppress("FunctionNaming")
fun NormalTS(
    priorMean: Double = 0.0,
    priorWeight: Double = 0.02,
    priorSquaredDeviations: Double = 0.02,
) = ThompsonSampling(NormalArm(priorMean, priorWeight, priorSquaredDeviations), NormalGammaPosterior)

@Suppress("FunctionNaming")
fun LogNormalTS(
    priorMean: Double = 0.0,
    priorWeight: Double = 0.02,
    priorSquaredDeviations: Double = 2.0,
) = ThompsonSampling(LogNormalArm(priorMean, priorWeight, priorSquaredDeviations), LogNormalGammaPosterior)

@Suppress("FunctionNaming")
fun PoissonTS(priorMean: Double = 1.0, priorWeight: Double = 0.01) =
    ThompsonSampling(MeanArm(priorMean, priorWeight), PoissonGammaPosterior)

@Suppress("FunctionNaming")
fun GeometricTS(priorMean: Double = 2.0, priorWeight: Double = 1.0) =
    ThompsonSampling(MeanArm(priorMean, priorWeight), GeometricBetaPosterior)

@Suppress("FunctionNaming")
fun ExponentialTS(priorMean: Double = 1.0, priorWeight: Double = 0.01) =
    ThompsonSampling(MeanArm(priorMean, priorWeight), ExponentialGammaPosterior)

@Suppress("FunctionNaming")
fun GammaScaleTS(fixedShape: Double, priorMean: Double = 1.0, priorWeight: Double = 0.1) =
    ThompsonSampling(MeanArm(priorMean, priorWeight), GammaScalePosterior(fixedShape))

// === UCB family ============================================================

class UCB1(
    val alpha: Double = 1.0,
    priorAlpha: Double = 1.0,
    priorBeta: Double = 1.0,
) : BanditPolicy<BernoulliSumResult> {
    override val arm = BernoulliArm(priorAlpha, priorBeta)
    private var totalSamples: Double = 0.0

    override fun update(stat: SeriesStat<BernoulliSumResult>, value: Double, weight: Double) {
        stat.update(arm.encode(value), 0L, weight)
        totalSamples += weight
    }
    override fun evaluate(snapshot: BernoulliSumResult, step: Long, maximize: Boolean, rng: Random): Double {
        val n = snapshot.trials
        if (n < 1.0) return Double.POSITIVE_INFINITY
        val mean = snapshot.successes / n
        val score = signedMean(mean, maximize)
        return score + alpha * sqrt(2 * ln(totalSamples) / n)
    }
    override fun addArm(snapshot: BernoulliSumResult) { totalSamples += snapshot.trials }
    override fun removeArm(snapshot: BernoulliSumResult) { totalSamples -= snapshot.trials }
}

class UCB1Normal(
    val alpha: Double = 1.0,
    priorMean: Double = 0.0,
    priorWeight: Double = 0.02,
) : BanditPolicy<MomentsResult> {
    override val arm = MomentsArm(priorMean, priorWeight)
    private var nbrArms = 0

    override fun evaluate(snapshot: MomentsResult, step: Long, maximize: Boolean, rng: Random): Double {
        val nj = snapshot.totalWeights
        if (nbrArms <= 1 || nj < 8 * ln(nbrArms.toDouble())) return Double.POSITIVE_INFINITY
        val score = signedMean(snapshot.mean, maximize)
        val mos = snapshot.meanOfSquares()
        val p1 = (mos - nj * snapshot.mean * snapshot.mean) / (nj - 1)
        return score + alpha * sqrt(16 * p1 * (ln(nbrArms - 1.0) / nj))
    }
    override fun addArm(snapshot: MomentsResult) { nbrArms++ }
    override fun removeArm(snapshot: MomentsResult) { nbrArms-- }
}

class UCB1Tuned(
    val alpha: Double = 1.0,
    priorMean: Double = 0.0,
    priorWeight: Double = 0.02,
) : BanditPolicy<MomentsResult> {
    override val arm = MomentsArm(priorMean, priorWeight)
    private var totalSamples: Double = 0.0

    override fun update(stat: SeriesStat<MomentsResult>, value: Double, weight: Double) {
        stat.update(arm.encode(value), 0L, weight)
        totalSamples += weight
    }
    override fun evaluate(snapshot: MomentsResult, step: Long, maximize: Boolean, rng: Random): Double {
        val nj = snapshot.totalWeights
        if (nj <= 1.0) return Double.POSITIVE_INFINITY
        val padding = ln(totalSamples) / nj
        val v = snapshot.meanOfSquares() - snapshot.mean * snapshot.mean + sqrt(2.0 * padding)
        val score = signedMean(snapshot.mean, maximize)
        return score + alpha * sqrt(padding * min(0.25, v))
    }
    override fun addArm(snapshot: MomentsResult) { totalSamples += snapshot.totalWeights }
    override fun removeArm(snapshot: MomentsResult) { totalSamples -= snapshot.totalWeights }
}

// === Mean-only policies (Normal arm, no sampling) ==========================

class Greedy(
    priorMean: Double = 0.0,
    priorWeight: Double = 0.02,
    priorSquaredDeviations: Double = 0.02,
) : BanditPolicy<WeightedVarianceResult> {
    override val arm = NormalArm(priorMean, priorWeight, priorSquaredDeviations)
    override fun evaluate(snapshot: WeightedVarianceResult, step: Long, maximize: Boolean, rng: Random) =
        signedMean(snapshot.mean, maximize)
}

class EpsilonGreedy(
    val epsilon: Double = 0.1,
    priorMean: Double = 0.0,
    priorWeight: Double = 0.02,
    priorSquaredDeviations: Double = 0.02,
) : BanditPolicy<WeightedVarianceResult> {
    init { require(epsilon in 0.0..1.0) { "epsilon must be in 0..1, got $epsilon" } }
    override val arm = NormalArm(priorMean, priorWeight, priorSquaredDeviations)

    override fun evaluate(snapshot: WeightedVarianceResult, step: Long, maximize: Boolean, rng: Random): Double {
        return if (Random(step).nextDouble() < epsilon) {
            rng.nextDouble()
        } else {
            signedMean(snapshot.mean, maximize)
        }
    }
}

class EpsilonDecreasing(
    val epsilon: Double = 2.0,
    val decay: Double = 0.5,
    priorMean: Double = 0.0,
    priorWeight: Double = 0.02,
    priorSquaredDeviations: Double = 0.02,
) : BanditPolicy<WeightedVarianceResult> {
    init { require(epsilon > 0.0) { "epsilon must be positive, got $epsilon" } }
    override val arm = NormalArm(priorMean, priorWeight, priorSquaredDeviations)
    private var totalSamples: Double = 0.0

    override fun update(stat: SeriesStat<WeightedVarianceResult>, value: Double, weight: Double) {
        stat.update(arm.encode(value), 0L, weight)
        totalSamples += weight
    }
    override fun evaluate(snapshot: WeightedVarianceResult, step: Long, maximize: Boolean, rng: Random): Double {
        val eps = min(1.0, epsilon / totalSamples.pow(decay))
        return if (Random(step).nextDouble() < eps) {
            rng.nextDouble()
        } else {
            signedMean(snapshot.mean, maximize)
        }
    }
    override fun addArm(snapshot: WeightedVarianceResult) { totalSamples += snapshot.totalWeights }
    override fun removeArm(snapshot: WeightedVarianceResult) { totalSamples -= snapshot.totalWeights }
}

class UniformSelection(
    priorMean: Double = 0.0,
    priorWeight: Double = 0.02,
    priorSquaredDeviations: Double = 0.02,
) : BanditPolicy<WeightedVarianceResult> {
    override val arm = NormalArm(priorMean, priorWeight, priorSquaredDeviations)
    override fun evaluate(snapshot: WeightedVarianceResult, step: Long, maximize: Boolean, rng: Random) =
        rng.nextDouble()
}
