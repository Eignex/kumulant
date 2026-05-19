package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stat.summary.BernoulliSumResult
import com.eignex.kumulant.stat.summary.MomentsResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
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
    /** Per-arm cumulator spec; determines the prior, encoding, and result shape. */
    val arm: Arm<R>

    /** Allocate a fresh per-arm accumulator from the [arm] spec. */
    fun createArm(): SeriesStat<R> = arm.createStat()

    /** Fold an observed reward [value] (with optional [weight]) into the per-arm [stat]. */
    fun update(stat: SeriesStat<R>, value: Double, weight: Double = 1.0) {
        stat.update(arm.encode(value), 0L, weight)
    }

    /** Score an arm given its current [snapshot]; higher scores are preferred by the bandit. */
    fun evaluate(snapshot: R, step: Long, rng: Random): Double

    /** Hook called when a new arm joins the population; default no-op. */
    fun addArm(snapshot: R) {}

    /** Hook called when an arm leaves the population; default no-op. */
    fun removeArm(snapshot: R) {}
}

/**
 * Thompson sampling: score each arm by a draw from its [posterior] given the snapshot.
 * Pair an [arm] spec with a posterior of the same [R].
 */
class ThompsonSampling<R : Result>(
    override val arm: Arm<R>,
    /** Stateless sampler used to draw a score from each arm's snapshot. */
    val posterior: Posterior<R>,
) : BanditPolicy<R> {
    override fun evaluate(snapshot: R, step: Long, rng: Random) =
        posterior.sample(snapshot, rng)
}

/** Thompson sampling over a Beta(`priorAlpha`, `priorBeta`) prior on a Bernoulli reward. */
@Suppress("FunctionNaming")
fun BetaBernoulliTS(priorAlpha: Double = 1.0, priorBeta: Double = 1.0) =
    ThompsonSampling(BernoulliArm(priorAlpha, priorBeta), BetaPosterior)

/** Thompson sampling over a Normal-Gamma prior; unknown mean and variance. */
@Suppress("FunctionNaming")
fun NormalTS(
    priorMean: Double = 0.0,
    priorWeight: Double = 0.02,
    priorSquaredDeviations: Double = 0.02,
) = ThompsonSampling(NormalArm(priorMean, priorWeight, priorSquaredDeviations), NormalGammaPosterior)

/** Thompson sampling over a log-normal reward via Normal-Gamma on `log(value)`. */
@Suppress("FunctionNaming")
fun LogNormalTS(
    priorMean: Double = 0.0,
    priorWeight: Double = 0.02,
    priorSquaredDeviations: Double = 2.0,
) = ThompsonSampling(LogNormalArm(priorMean, priorWeight, priorSquaredDeviations), LogNormalGammaPosterior)

/** Thompson sampling over a Poisson reward with a Gamma prior on the rate. */
@Suppress("FunctionNaming")
fun PoissonTS(priorMean: Double = 1.0, priorWeight: Double = 0.01) =
    ThompsonSampling(MeanArm(priorMean, priorWeight), PoissonGammaPosterior)

/** Thompson sampling over a geometric reward with a Beta prior on the success probability. */
@Suppress("FunctionNaming")
fun GeometricTS(priorMean: Double = 2.0, priorWeight: Double = 1.0) =
    ThompsonSampling(MeanArm(priorMean, priorWeight), GeometricBetaPosterior)

/** Thompson sampling over an exponential reward with a Gamma prior on the rate. */
@Suppress("FunctionNaming")
fun ExponentialTS(priorMean: Double = 1.0, priorWeight: Double = 0.01) =
    ThompsonSampling(MeanArm(priorMean, priorWeight), ExponentialGammaPosterior)

/** Thompson sampling over a Gamma reward with known shape and Gamma prior on the scale. */
@Suppress("FunctionNaming")
fun GammaScaleTS(fixedShape: Double, priorMean: Double = 1.0, priorWeight: Double = 0.1) =
    ThompsonSampling(MeanArm(priorMean, priorWeight), GammaScalePosterior(fixedShape))

/** Classical UCB1 over a Bernoulli reward with a Beta prior on the success probability. */
class UCB1(
    /** Exploration scale on the confidence-bound term. */
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
    override fun evaluate(snapshot: BernoulliSumResult, step: Long, rng: Random): Double {
        val n = snapshot.trials
        if (n < 1.0) return Double.POSITIVE_INFINITY
        val mean = snapshot.successes / n
        return mean + alpha * sqrt(2 * ln(totalSamples) / n)
    }
    override fun addArm(snapshot: BernoulliSumResult) { totalSamples += snapshot.trials }
    override fun removeArm(snapshot: BernoulliSumResult) { totalSamples -= snapshot.trials }
}

/** UCB1-Normal: Auer et al.'s variance-aware UCB for Gaussian rewards. */
class UCB1Normal(
    /** Exploration scale on the confidence-bound term. */
    val alpha: Double = 1.0,
    priorMean: Double = 0.0,
    priorWeight: Double = 0.02,
) : BanditPolicy<MomentsResult> {
    override val arm = MomentsArm(priorMean, priorWeight)
    private var nbrArms = 0

    override fun evaluate(snapshot: MomentsResult, step: Long, rng: Random): Double {
        val nj = snapshot.totalWeights
        if (nbrArms <= 1 || nj < 8 * ln(nbrArms.toDouble())) return Double.POSITIVE_INFINITY
        val mos = snapshot.meanOfSquares()
        val p1 = (mos - nj * snapshot.mean * snapshot.mean) / (nj - 1)
        return snapshot.mean + alpha * sqrt(16 * p1 * (ln(nbrArms - 1.0) / nj))
    }
    override fun addArm(snapshot: MomentsResult) { nbrArms++ }
    override fun removeArm(snapshot: MomentsResult) { nbrArms-- }
}

/** UCB1-Tuned: Auer et al.'s sample-variance-aware UCB, often tighter than plain UCB1. */
class UCB1Tuned(
    /** Exploration scale on the confidence-bound term. */
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
    override fun evaluate(snapshot: MomentsResult, step: Long, rng: Random): Double {
        val nj = snapshot.totalWeights
        if (nj <= 1.0) return Double.POSITIVE_INFINITY
        val padding = ln(totalSamples) / nj
        val v = snapshot.meanOfSquares() - snapshot.mean * snapshot.mean + sqrt(2.0 * padding)
        return snapshot.mean + alpha * sqrt(padding * min(0.25, v))
    }
    override fun addArm(snapshot: MomentsResult) { totalSamples += snapshot.totalWeights }
    override fun removeArm(snapshot: MomentsResult) { totalSamples -= snapshot.totalWeights }
}

/** Pure-exploitation policy: always picks the arm with the highest posterior mean. */
class Greedy(
    priorMean: Double = 0.0,
    priorWeight: Double = 0.02,
    priorSquaredDeviations: Double = 0.02,
) : BanditPolicy<WeightedVarianceResult> {
    override val arm = NormalArm(priorMean, priorWeight, priorSquaredDeviations)
    override fun evaluate(snapshot: WeightedVarianceResult, step: Long, rng: Random) =
        snapshot.mean
}

/** Epsilon-greedy: with probability [epsilon] pick uniformly, otherwise pick the highest mean. */
class EpsilonGreedy(
    /** Probability of exploring uniformly instead of exploiting the best mean. */
    val epsilon: Double = 0.1,
    priorMean: Double = 0.0,
    priorWeight: Double = 0.02,
    priorSquaredDeviations: Double = 0.02,
) : BanditPolicy<WeightedVarianceResult> {
    init { require(epsilon in 0.0..1.0) { "epsilon must be in 0..1, got $epsilon" } }
    override val arm = NormalArm(priorMean, priorWeight, priorSquaredDeviations)

    override fun evaluate(snapshot: WeightedVarianceResult, step: Long, rng: Random): Double {
        return if (Random(step).nextDouble() < epsilon) {
            rng.nextDouble()
        } else {
            snapshot.mean
        }
    }
}

/** Epsilon-greedy with `epsilon_t = min(1, epsilon / totalSamples^decay)`. */
class EpsilonDecreasing(
    /** Initial exploration scale; effective epsilon decays as samples accumulate. */
    val epsilon: Double = 2.0,
    /** Decay exponent applied to the running sample count. */
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
    override fun evaluate(snapshot: WeightedVarianceResult, step: Long, rng: Random): Double {
        val eps = min(1.0, epsilon / totalSamples.pow(decay))
        return if (Random(step).nextDouble() < eps) {
            rng.nextDouble()
        } else {
            snapshot.mean
        }
    }
    override fun addArm(snapshot: WeightedVarianceResult) { totalSamples += snapshot.totalWeights }
    override fun removeArm(snapshot: WeightedVarianceResult) { totalSamples -= snapshot.totalWeights }
}

/** Pure-exploration policy: every evaluate returns a fresh uniform draw. */
class UniformSelection(
    priorMean: Double = 0.0,
    priorWeight: Double = 0.02,
    priorSquaredDeviations: Double = 0.02,
) : BanditPolicy<WeightedVarianceResult> {
    override val arm = NormalArm(priorMean, priorWeight, priorSquaredDeviations)
    override fun evaluate(snapshot: WeightedVarianceResult, step: Long, rng: Random) =
        rng.nextDouble()
}

/**
 * KL-UCB (Garivier & Cappé 2011) — UCB variant for Bernoulli arms with a KL-divergence
 * confidence bound. Score is `sup { q in [mean, 1] : n * KL(mean, q) <= ln(t) + c*ln(ln(t)) }`
 * computed by binary search. Asymptotically optimal for Bernoulli rewards.
 */
class KlUcb(
    /** Confidence padding: `ln(t) + c * ln(ln(t))`. Default `c = 0` is the standard form. */
    val c: Double = 0.0,
    /** Binary-search tolerance for the quantile root. */
    val tolerance: Double = 1e-6,
    priorAlpha: Double = 1.0,
    priorBeta: Double = 1.0,
) : BanditPolicy<BernoulliSumResult> {
    override val arm = BernoulliArm(priorAlpha, priorBeta)
    private var totalSamples: Double = 0.0

    override fun update(stat: SeriesStat<BernoulliSumResult>, value: Double, weight: Double) {
        stat.update(arm.encode(value), 0L, weight)
        totalSamples += weight
    }

    override fun evaluate(snapshot: BernoulliSumResult, step: Long, rng: Random): Double {
        val n = snapshot.trials
        if (n < 1.0 || totalSamples <= 1.0) return Double.POSITIVE_INFINITY
        val mean = (snapshot.successes / n).coerceIn(0.0, 1.0)
        val bound = (ln(totalSamples) + c * ln(ln(totalSamples).coerceAtLeast(1.0))) / n
        return klBernoulliUpper(mean, bound, tolerance)
    }

    override fun addArm(snapshot: BernoulliSumResult) { totalSamples += snapshot.trials }
    override fun removeArm(snapshot: BernoulliSumResult) { totalSamples -= snapshot.trials }

    /** Bernoulli KL utilities used by [KlUcb]. */
    companion object {
        /** `sup { q in [p, 1] : KL(p, q) <= bound }` via bisection. */
        fun klBernoulliUpper(p: Double, bound: Double, tol: Double): Double {
            if (bound <= 0.0) return p
            var lo = p
            var hi = 1.0
            while (hi - lo > tol) {
                val mid = (lo + hi) * 0.5
                if (klBernoulli(p, mid) > bound) hi = mid else lo = mid
            }
            return lo
        }

        /** KL divergence between two Bernoulli distributions with means [p] and [q]. */
        fun klBernoulli(p: Double, q: Double): Double {
            if (q <= 0.0 || q >= 1.0) return Double.POSITIVE_INFINITY
            var s = 0.0
            if (p > 0.0) s += p * ln(p / q)
            if (p < 1.0) s += (1.0 - p) * ln((1.0 - p) / (1.0 - q))
            return s
        }
    }
}

/**
 * MOSS (Audibert & Bubeck 2009) — minimax-optimal UCB variant. Score is
 * `mean + sqrt(max(0, ln(t / (K * n))) / n)`. The bound shrinks faster than UCB1
 * once an arm has more than `t / K` samples, eliminating the `log(t)` term's slack.
 * Uses the anytime form (no horizon argument).
 */
class Moss(
    /** Number of arms in the population; used in the bound's `t / (K * n)` term. */
    val nbrArms: Int,
    priorMean: Double = 0.0,
    priorWeight: Double = 0.02,
) : BanditPolicy<WeightedMeanResult> {
    init { require(nbrArms > 0) { "nbrArms must be positive, got $nbrArms" } }
    override val arm = MeanArm(priorMean, priorWeight)
    private var totalSamples: Double = 0.0

    override fun update(stat: SeriesStat<WeightedMeanResult>, value: Double, weight: Double) {
        stat.update(arm.encode(value), 0L, weight)
        totalSamples += weight
    }

    override fun evaluate(snapshot: WeightedMeanResult, step: Long, rng: Random): Double {
        val n = snapshot.totalWeights
        if (n < 1.0) return Double.POSITIVE_INFINITY
        val arg = totalSamples / (nbrArms * n)
        val padding = ln(arg.coerceAtLeast(1.0))
        return snapshot.mean + sqrt(padding / n)
    }

    override fun addArm(snapshot: WeightedMeanResult) { totalSamples += snapshot.totalWeights }
    override fun removeArm(snapshot: WeightedMeanResult) { totalSamples -= snapshot.totalWeights }
}

/**
 * UCB-V (Audibert, Munos, Szepesvári 2009) — variance-aware UCB. Score is
 * `mean + sqrt(2 * V * zeta * ln(t) / n) + 3 * c * zeta * ln(t) / n`. The bias-
 * correction third term makes the bound finite-sample-honest where UCB1-Tuned's
 * variance-aware bound is only asymptotic.
 */
class UcbV(
    /** Variance-term scale. Audibert et al. recommend `zeta in [1, 1.2]`. */
    val zeta: Double = 1.2,
    /** Bias-correction term scale. Default matches the original paper. */
    val c: Double = 1.0,
    priorMean: Double = 0.0,
    priorWeight: Double = 0.02,
) : BanditPolicy<MomentsResult> {
    init { require(zeta > 0.0) { "zeta must be positive, got $zeta" } }
    override val arm = MomentsArm(priorMean, priorWeight)
    private var totalSamples: Double = 0.0

    override fun update(stat: SeriesStat<MomentsResult>, value: Double, weight: Double) {
        stat.update(arm.encode(value), 0L, weight)
        totalSamples += weight
    }

    override fun evaluate(snapshot: MomentsResult, step: Long, rng: Random): Double {
        val n = snapshot.totalWeights
        if (n < 1.0) return Double.POSITIVE_INFINITY
        val v = (snapshot.meanOfSquares() - snapshot.mean * snapshot.mean).coerceAtLeast(0.0)
        val logT = ln(totalSamples.coerceAtLeast(2.0))
        return snapshot.mean + sqrt(2.0 * v * zeta * logT / n) + 3.0 * c * zeta * logT / n
    }

    override fun addArm(snapshot: MomentsResult) { totalSamples += snapshot.totalWeights }
    override fun removeArm(snapshot: MomentsResult) { totalSamples -= snapshot.totalWeights }
}
