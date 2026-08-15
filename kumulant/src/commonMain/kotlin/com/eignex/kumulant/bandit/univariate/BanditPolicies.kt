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
 * Scoring strategy for a [com.eignex.kumulant.bandit.univariate.MultiArmedBandit].
 * Decides which arm to play given snapshots of each arm's sufficient statistic
 * [R]. The bandit calls [evaluate] for every arm and picks the argmax; the
 * policy is the entire exploration/exploitation knob.
 *
 * The policy owns the per-arm cumulator lifecycle through its [arm] spec:
 *
 * - [createArm] returns a freshly-prior-seeded [SeriesStat] from
 *   `arm.createStat()`.
 * - [update] folds an observation in, applying `arm.encode` first so the
 *   stat sees the encoded value (e.g. `ln(value)` for [LogNormalArm]).
 * - [evaluate] reads the resulting snapshot.
 *
 * Two flavours:
 *
 * - **Sampling-based** ([ThompsonSampling]): score each arm by a draw from
 *   its conjugate [Posterior] given the snapshot. Exploration is implicit
 *   in posterior variance: under-explored arms have wider posteriors and
 *   draw higher scores more often.
 * - **UCB-based** ([UCB1], [UCB1Normal], [UCB1Tuned], [UcbV], [KlUcb], [Moss])
 *  ; score is `mean + alpha * confidence-bound` derived from the snapshot
 *   directly. Exploration is explicit in the confidence width.
 *
 * Per-policy global state (e.g. total samples for UCB) updates through
 * [addArm] / [removeArm] when the arm population changes mid-run, and
 * through [update]'s side effects on each observation.
 */
interface BanditPolicy<R : Result> {
    /**
     * Per-arm cumulator spec; determines the prior pseudo-counts, value
     * encoding, and result shape that [evaluate] consumes.
     */
    val arm: Arm<R>

    /**
     * Allocate a fresh per-arm accumulator from the [arm] spec. Default
     * delegates to `arm.createStat()`; override only if the policy needs a
     * non-standard variant.
     */
    fun createArm(): SeriesStat<R> = arm.createStat()

    /**
     * Fold an observed reward [value] (with optional [weight]) into the
     * per-arm [stat]. Default applies `arm.encode` first; policies with
     * global counters (UCB families) override to update their counter
     * alongside the stat update.
     */
    fun update(stat: SeriesStat<R>, value: Double, weight: Double = 1.0) {
        stat.update(arm.encode(value), 0L, weight)
    }

    /**
     * Score an arm given its current [snapshot]. Higher scores are preferred
     * by the bandit. [step] is the global update count (for time-dependent
     * exploration schedules); [rng] is the bandit's shared
     * [com.eignex.kumulant.bandit.Bandit.random] (consumed by sampling
     * policies).
     */
    fun evaluate(snapshot: R, step: Long, rng: Random): Double

    /**
     * Hook called when a new arm joins the population. Lets stateful
     * policies fold the new arm's snapshot into their global counters
     * (UCB's total-samples, UCB1Normal's arm count). Default no-op.
     */
    fun addArm(snapshot: R) {}

    /**
     * Hook called when an arm leaves the population. Inverse of [addArm];
     * lets stateful policies remove the departing arm's contribution from
     * their global counters. Default no-op.
     */
    fun removeArm(snapshot: R) {}
}

/**
 * Thompson sampling: score each arm by a draw from its conjugate
 * [posterior] given the snapshot. The bandit then picks the arm with the
 * highest sample; no explicit exploration knob, the exploration falls out
 * of posterior variance shrinking as data accumulates.
 *
 * Pair an [Arm] with a [Posterior] of the same result type [R]:
 *
 * - [BernoulliArm] + [BetaPosterior]: see [BetaBernoulliTS].
 * - [NormalArm] + [NormalGammaPosterior]: see [NormalTS].
 * - [LogNormalArm] + [LogNormalGammaPosterior]: see [LogNormalTS].
 * - [MeanArm] + [PoissonGammaPosterior] / [GeometricBetaPosterior] /
 *   [ExponentialGammaPosterior] / [GammaScalePosterior]; see [PoissonTS],
 *   [GeometricTS], [ExponentialTS], [GammaScaleTS].
 *
 * Stateless across arms; [addArm] / [removeArm] are no-ops because no
 * global counter is involved.
 */
class ThompsonSampling<R : Result>(
    override val arm: Arm<R>,
    /** Stateless sampler used to draw a score from each arm's snapshot. */
    val posterior: Posterior<R>,
) : BanditPolicy<R> {
    override fun evaluate(snapshot: R, step: Long, rng: Random) = posterior.sample(snapshot, rng)
}

/** Thompson sampling over a Beta(`priorAlpha`, `priorBeta`) prior on a Bernoulli reward. */
@Suppress("FunctionNaming")
fun BetaBernoulliTS(priorAlpha: Double = 1.0, priorBeta: Double = 1.0) =
    ThompsonSampling(BernoulliArm(priorAlpha, priorBeta), BetaPosterior)

/** Thompson sampling over a Normal-Gamma prior; unknown mean and variance. */
@Suppress("FunctionNaming")
fun NormalTS(priorMean: Double = 0.0, priorWeight: Double = 0.02, priorSquaredDeviations: Double = 0.02) =
    ThompsonSampling(NormalArm(priorMean, priorWeight, priorSquaredDeviations), NormalGammaPosterior)

/** Thompson sampling over a log-normal reward via Normal-Gamma on `log(value)`. */
@Suppress("FunctionNaming")
fun LogNormalTS(priorMean: Double = 0.0, priorWeight: Double = 0.02, priorSquaredDeviations: Double = 2.0) =
    ThompsonSampling(LogNormalArm(priorMean, priorWeight, priorSquaredDeviations), LogNormalGammaPosterior)

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

/**
 * Classical UCB1 (Auer, Cesa-Bianchi, Fischer 2002). Score is
 * `mean + alpha * sqrt(2 * ln(totalSamples) / armSamples)`; exploitation
 * (running mean) plus a confidence bound that shrinks as the arm
 * accumulates pulls. Unexplored arms get `+infinity` so they're tried at
 * least once.
 *
 * Designed for Bernoulli rewards but works on any `[0, 1]`-bounded reward.
 * The exploration constant [alpha] scales the confidence width; the
 * theoretical value is 1.0, lower values reduce exploration, higher
 * increases it.
 *
 * Pair this with a [BernoulliArm] beta prior; the prior alpha/beta seed
 * the snapshot so the first few pulls aren't dominated by integer noise.
 */
class UCB1(
    /** Exploration scale on the confidence-bound term. Theoretical default is 1.0. */
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
    override fun addArm(snapshot: BernoulliSumResult) {
        totalSamples += snapshot.trials
    }
    override fun removeArm(snapshot: BernoulliSumResult) {
        totalSamples -= snapshot.trials
    }
}

/**
 * UCB1-Normal (Auer et al. 2002). Variance-aware UCB for Gaussian rewards;
 * uses the sample variance derived from the [MomentsResult] snapshot to
 * scale the confidence bound. Reach for it when rewards are roughly
 * Gaussian and unbounded; [UCB1]'s `[0, 1]` assumption doesn't hold.
 *
 * Forces exploration until each arm has at least `8 * ln(K)` pulls (`K` is
 * the arm count), then switches to the variance-aware score
 * `mean + alpha * sqrt(16 * variance * ln(K - 1) / armSamples)`.
 */
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
        // Auer et al. subtract `n * mean^2` from the SUM of squares, and meanOfSquares() is the
        // MEAN of squares, so this has to scale up by `nj` first. Subtracting `n * mean^2` from
        // `E[x^2]` made p1 negative for every arm with a non-zero mean, so the sqrt below was NaN
        // and `choose` - which seeds its best score at -Infinity, and `NaN > -Inf` is false -
        // silently degenerated to always picking arm 0.
        val sumOfSquares = snapshot.meanOfSquares() * nj
        val p1 = ((sumOfSquares - nj * snapshot.mean * snapshot.mean) / (nj - 1)).coerceAtLeast(0.0)
        return snapshot.mean + alpha * sqrt(16 * p1 * (ln(nbrArms - 1.0) / nj))
    }
    override fun addArm(snapshot: MomentsResult) {
        nbrArms++
    }
    override fun removeArm(snapshot: MomentsResult) {
        nbrArms--
    }
}

/**
 * UCB1-Tuned (Auer et al. 2002). Same shape as [UCB1] but the confidence
 * bound multiplier uses an upper bound on the variance: `min(0.25, v)`
 * where `v` is the sample variance plus a small padding term. Tighter
 * bound than plain [UCB1] when the empirical variance is well below
 * `0.25`; degrades gracefully to [UCB1] when the variance is uninformative.
 *
 * Designed for `[0, 1]`-bounded rewards (the `0.25` ceiling assumes
 * variance can't exceed `1/4`). For Gaussian unbounded rewards reach for
 * [UCB1Normal] instead.
 */
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
    override fun addArm(snapshot: MomentsResult) {
        totalSamples += snapshot.totalWeights
    }
    override fun removeArm(snapshot: MomentsResult) {
        totalSamples -= snapshot.totalWeights
    }
}

/**
 * Pure-exploitation policy: always picks the arm with the highest posterior
 * mean. No exploration at all; converges fastest to the apparent best arm
 * but can lock into a suboptimal arm forever if early rewards mislead it.
 *
 * Useful as a baseline (regret comparison against random / UCB / Thompson)
 * and as a quick-and-dirty starting policy when prior beliefs are already
 * good. For real online learning use one of the exploration-bearing
 * policies instead.
 */
class Greedy(priorMean: Double = 0.0, priorWeight: Double = 0.02, priorSquaredDeviations: Double = 0.02) :
    BanditPolicy<WeightedVarianceResult> {
    override val arm = NormalArm(priorMean, priorWeight, priorSquaredDeviations)
    override fun evaluate(snapshot: WeightedVarianceResult, step: Long, rng: Random) = snapshot.mean
}

/**
 * Epsilon-greedy: with probability [epsilon] pick a uniformly random arm
 * (explore), otherwise pick the arm with the highest mean (exploit).
 * The simplest exploration scheme that actually works; no math machinery,
 * tune one knob.
 *
 * Sensitive to the epsilon value: too low and you under-explore (regret
 * scales linearly in horizon for the wrong arm); too high and you waste
 * pulls on known-bad arms. Typical good values are `0.05`–`0.2`. For
 * automatic tuning use [EpsilonDecreasing], which anneals epsilon toward
 * zero as samples accumulate.
 */
class EpsilonGreedy(
    /** Probability of exploring uniformly instead of exploiting the best mean. */
    val epsilon: Double = 0.1,
    priorMean: Double = 0.0,
    priorWeight: Double = 0.02,
    priorSquaredDeviations: Double = 0.02,
) : BanditPolicy<WeightedVarianceResult> {
    init {
        require(epsilon in 0.0..1.0) { "epsilon must be in 0..1, got $epsilon" }
    }
    override val arm = NormalArm(priorMean, priorWeight, priorSquaredDeviations)

    override fun evaluate(snapshot: WeightedVarianceResult, step: Long, rng: Random): Double =
        if (Random(step).nextDouble() < epsilon) {
            rng.nextDouble()
        } else {
            snapshot.mean
        }
}

/**
 * Annealed epsilon-greedy. Effective exploration probability decreases as
 * sample count accumulates: `eps(t) = min(1, epsilon / totalSamples^decay)`.
 *
 * Solves [EpsilonGreedy]'s fixed-epsilon trade-off: explore aggressively
 * early, then converge to mostly-greedy once the per-arm posteriors are
 * well-separated. Theoretical defaults give `decay = 0.5` (Auer et al.
 * 2002) for a `sqrt(T)` regret bound; lower `decay` keeps exploring
 * longer, higher `decay` converges to greedy faster.
 */
class EpsilonDecreasing(
    /** Initial exploration scale; effective epsilon decays as samples accumulate. */
    val epsilon: Double = 2.0,
    /** Decay exponent applied to the running sample count. */
    val decay: Double = 0.5,
    priorMean: Double = 0.0,
    priorWeight: Double = 0.02,
    priorSquaredDeviations: Double = 0.02,
) : BanditPolicy<WeightedVarianceResult> {
    init {
        require(epsilon > 0.0) { "epsilon must be positive, got $epsilon" }
    }
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
    override fun addArm(snapshot: WeightedVarianceResult) {
        totalSamples += snapshot.totalWeights
    }
    override fun removeArm(snapshot: WeightedVarianceResult) {
        totalSamples -= snapshot.totalWeights
    }
}

/**
 * Pure-exploration policy: every evaluate returns a fresh uniform draw,
 * so the bandit picks arms uniformly at random regardless of observations.
 * No exploitation at all; the opposite extreme of [Greedy].
 *
 * Useful as a regret-comparison baseline (any policy worth its salt
 * should beat uniform), as a data-collection scheme before switching to
 * a real policy, and as the inner uniformly-random arm in
 * [EpsilonGreedy] / [EpsilonDecreasing]. Not a real online-learning
 * choice on its own.
 */
class UniformSelection(priorMean: Double = 0.0, priorWeight: Double = 0.02, priorSquaredDeviations: Double = 0.02) :
    BanditPolicy<WeightedVarianceResult> {
    override val arm = NormalArm(priorMean, priorWeight, priorSquaredDeviations)
    override fun evaluate(snapshot: WeightedVarianceResult, step: Long, rng: Random) = rng.nextDouble()
}

/**
 * KL-UCB (Garivier & Cappé 2011). UCB variant for Bernoulli arms with a
 * KL-divergence confidence bound instead of the Hoeffding bound [UCB1]
 * uses. Score is the largest `q` in `[mean, 1]` such that
 * `n * KL(mean, q) <= ln(t) + c * ln(ln(t))`; computed by binary search
 * with [tolerance] precision.
 *
 * Asymptotically optimal for Bernoulli rewards; the bound matches
 * Lai-Robbins lower regret in the limit. Beats [UCB1] in practice when
 * rewards are genuinely Bernoulli; falls back to similar regret when
 * rewards are bounded but not Bernoulli.
 *
 * Per-evaluate cost is O(log(1/tolerance)) for the binary search; with
 * default `tolerance = 1e-6` that's ~20 steps, each constant-time.
 * Cheaper than full Thompson but more expensive than [UCB1].
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

    override fun addArm(snapshot: BernoulliSumResult) {
        totalSamples += snapshot.trials
    }
    override fun removeArm(snapshot: BernoulliSumResult) {
        totalSamples -= snapshot.trials
    }

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
 * MOSS; Minimax Optimal Strategy in the Stochastic case (Audibert & Bubeck
 * 2009). UCB variant where the confidence bound shrinks faster than [UCB1]
 * once an arm has accumulated more than `t / K` samples. Score is
 * `mean + sqrt(max(0, ln(t / (K * n))) / n)`.
 *
 * Achieves minimax-optimal regret in the stochastic bandit setting;
 * tighter worst-case bound than [UCB1] across all reward distributions
 * the bandit could face. Eliminates the `log(t)` slack term once an arm
 * is sampled enough.
 *
 * Uses the anytime form (no fixed horizon argument). [nbrArms] is needed
 * to compute the `t / K` denominator; pass the same arm count the
 * containing [MultiArmedBandit] uses.
 *
 * Reach for it when minimax regret matters more than asymptotic optimality
 *; adversarial reward distributions, settings where the worst case
 * matters. For Bernoulli rewards specifically, [KlUcb] is asymptotically
 * tighter.
 */
class Moss(
    /** Number of arms in the population; used in the bound's `t / (K * n)` term. */
    val nbrArms: Int,
    priorMean: Double = 0.0,
    priorWeight: Double = 0.02,
) : BanditPolicy<WeightedMeanResult> {
    init {
        require(nbrArms > 0) { "nbrArms must be positive, got $nbrArms" }
    }
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

    override fun addArm(snapshot: WeightedMeanResult) {
        totalSamples += snapshot.totalWeights
    }
    override fun removeArm(snapshot: WeightedMeanResult) {
        totalSamples -= snapshot.totalWeights
    }
}

/**
 * UCB-V; variance-aware UCB with finite-sample honesty (Audibert, Munos,
 * Szepesvári 2009). Score is
 * `mean + sqrt(2 * V * zeta * ln(t) / n) + 3 * c * zeta * ln(t) / n`,
 * where `V` is the running variance from the [MomentsResult] snapshot.
 *
 * The third term; a bias correction scaled by [c]; is what distinguishes
 * UCB-V from [UCB1Tuned]: the bound is honest at finite sample sizes
 * rather than only asymptotically. Reach for it when sample sizes per arm
 * stay small (early stopping, expensive arms) and the asymptotic
 * tightness of [UCB1Tuned] / [KlUcb] doesn't materialise.
 *
 * Audibert et al. recommend `zeta in [1, 1.2]`; defaults are at the upper
 * end of that range.
 */
class UcbV(
    /** Variance-term scale. Audibert et al. recommend `zeta in [1, 1.2]`. */
    val zeta: Double = 1.2,
    /** Bias-correction term scale. Default matches the original paper. */
    val c: Double = 1.0,
    priorMean: Double = 0.0,
    priorWeight: Double = 0.02,
) : BanditPolicy<MomentsResult> {
    init {
        require(zeta > 0.0) { "zeta must be positive, got $zeta" }
    }
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

    override fun addArm(snapshot: MomentsResult) {
        totalSamples += snapshot.totalWeights
    }
    override fun removeArm(snapshot: MomentsResult) {
        totalSamples -= snapshot.totalWeights
    }
}
