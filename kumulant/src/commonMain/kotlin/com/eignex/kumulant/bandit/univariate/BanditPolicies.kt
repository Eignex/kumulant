package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.bandit.requireNbrArms
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stat.summary.BernoulliSumResult
import com.eignex.kumulant.stat.summary.MomentsResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import com.eignex.kumulant.stream.AtomicMode
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
 * - **UCB-based** ([UCB1], [UcbV], [KlUcb], [Moss])
 *   score is `mean + alpha * confidence-bound` derived from the snapshot
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
     * Allocate a fresh per-arm accumulator from the [arm] spec with the given
     * [concurrency]. Default delegates to `arm.createStat(concurrency)`;
     * override only if the policy needs a non-standard variant.
     */
    fun createArm(concurrency: Concurrency = Concurrency.None): SeriesStat<R> = arm.createStat(concurrency)

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
     * (UCB's total-samples). Default no-op.
     */
    fun addArm(snapshot: R) {}

    /**
     * Hook called when an arm leaves the population. Inverse of [addArm];
     * lets stateful policies remove the departing arm's contribution from
     * their global counters. Default no-op.
     */
    fun removeArm(snapshot: R) {}

    /**
     * Fresh policy carrying the same configuration but no accumulated global state.
     * A replica bandit must not share counters with the bandit it was created from,
     * so any policy holding mutable state has to override this; the default returns
     * `this` because a stateless policy is safe to share.
     */
    fun createPolicy(): BanditPolicy<R> = this
}

/**
 * Base for policies whose score depends on the population's total sample weight (the UCB family
 * and annealed epsilon-greedy). Tracks that total and the arm count through [update], [addArm]
 * and [removeArm]; [sampleWeight] reads an arm snapshot's share of the total.
 *
 * The counters are shared across arms, so concurrent updates use atomic cells.
 */
abstract class SampleCountingPolicy<R : Result>(private val sampleWeight: (R) -> Double) : BanditPolicy<R> {
    private val samples = AtomicMode.newDouble(0.0)
    private val arms = AtomicMode.newLong(0L)

    /** Total sample weight across every arm, priors included. */
    protected val totalSamples: Double get() = samples.load()

    /** Number of arms currently in the population. */
    protected val armCount: Int get() = arms.load().toInt()

    override fun update(stat: SeriesStat<R>, value: Double, weight: Double) {
        super.update(stat, value, weight)
        samples.add(weight)
    }

    override fun addArm(snapshot: R) {
        samples.add(sampleWeight(snapshot))
        arms.add(1L)
    }

    override fun removeArm(snapshot: R) {
        samples.add(-sampleWeight(snapshot))
        arms.add(-1L)
    }
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
 * - [MeanArm] + [PoissonGammaPosterior] / [ExponentialGammaPosterior]; see
 *   [PoissonTS], [ExponentialTS].
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

/** Thompson sampling over an exponential reward with a Gamma prior on the rate. */
@Suppress("FunctionNaming")
fun ExponentialTS(priorMean: Double = 1.0, priorWeight: Double = 0.01) =
    ThompsonSampling(MeanArm(priorMean, priorWeight), ExponentialGammaPosterior)

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
) : SampleCountingPolicy<BernoulliSumResult>({ it.trials }) {
    override val arm = BernoulliArm(priorAlpha, priorBeta)

    override fun createPolicy() = UCB1(alpha, arm.priorAlpha, arm.priorBeta)

    override fun evaluate(snapshot: BernoulliSumResult, step: Long, rng: Random): Double {
        val n = snapshot.trials
        if (n < 1.0) return Double.POSITIVE_INFINITY
        val mean = snapshot.successes / n
        return mean + alpha * sqrt(2 * ln(totalSamples) / n)
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
class Greedy(priorMean: Double = 0.0, priorWeight: Double = 0.02) : BanditPolicy<WeightedVarianceResult> {
    override val arm = NormalArm(priorMean, priorWeight, priorSquaredDeviations = 0.0)
    override fun evaluate(snapshot: WeightedVarianceResult, step: Long, rng: Random) = snapshot.mean
}

// Share one exploration decision across all arms in a round so argmax cannot mix
// exploration scores with exploitation means. Draw from the caller's RNG rather
// than seeding by step, which would make different seeds explore in lockstep.
// Without a new step, direct evaluate calls reuse the draw; racing rounds may share one.
internal class RoundCoin {
    private var round = Long.MIN_VALUE
    private var draw = 0.0

    fun explores(step: Long, rng: Random, epsilon: Double): Boolean {
        if (step != round) {
            round = step
            draw = rng.nextDouble()
        }
        return draw < epsilon
    }
}

/**
 * Epsilon-greedy: with probability `eps` pick a uniformly random arm
 * (explore), otherwise pick the arm with the highest mean (exploit).
 * The simplest exploration scheme that actually works; no math machinery.
 *
 * The exploration probability is `eps(n) = min(1, epsilon / n^decay)` over the total
 * sample count `n`. At the default `decay = 0` it is the fixed [epsilon]; typical good
 * values are `0.05`-`0.2`. A positive [decay] anneals exploration toward zero as samples
 * accumulate, exploring aggressively early and converging to greedy once the arms are
 * well separated; `decay = 0.5` with `epsilon` around `2.0` gives Auer et al.'s
 * `sqrt(T)` regret bound.
 */
class EpsilonGreedy(
    /** Exploration probability at `decay = 0`; otherwise the scale of the annealed probability. */
    val epsilon: Double = 0.1,
    /** Decay exponent applied to the total sample count; `0.0` keeps exploration fixed. */
    val decay: Double = 0.0,
    priorMean: Double = 0.0,
    priorWeight: Double = 0.02,
) : SampleCountingPolicy<WeightedVarianceResult>({ it.totalWeights }) {
    init {
        require(decay >= 0.0) { "decay must be non-negative, got $decay" }
        require(epsilon >= 0.0 && (decay > 0.0 || epsilon <= 1.0)) {
            "epsilon must be in 0..1 at decay 0, or non-negative with a positive decay, got $epsilon"
        }
    }
    override val arm = NormalArm(priorMean, priorWeight, priorSquaredDeviations = 0.0)

    private val coin = RoundCoin()
    override fun createPolicy() =
        EpsilonGreedy(epsilon, decay, arm.priorMean, arm.priorWeight)

    override fun evaluate(snapshot: WeightedVarianceResult, step: Long, rng: Random): Double {
        val eps = if (decay == 0.0) epsilon else min(1.0, epsilon / totalSamples.pow(decay))
        return if (coin.explores(step, rng, eps)) rng.nextDouble() else snapshot.mean
    }
}

/**
 * Boltzmann exploration (softmax bandit): plays arm `a` with probability proportional to
 * `exp(mean(a) / tau(t))`, where the temperature cools as
 * `tau(t) = max(minTau, initialTau / t^decay)` over the 1-based round `t`; `decay = 1`
 * is Cesa-Bianchi & Fischer's classical recipe and `decay = 0` a fixed temperature.
 * High temperature flattens play toward uniform exploration; low temperature sharpens
 * it toward greedy exploitation.
 *
 * The softmax draw is the argmax of `mean / tau` plus independent standard Gumbel noise
 * per arm, which samples the softmax distribution exactly, so the policy scores arms
 * independently inside [MultiArmedBandit]. A non-finite mean scores as logit `0`, so a
 * corrupt arm stays playable without dominating.
 *
 * **Use cases:** stationary scalar-reward problems where smooth probabilistic
 * exploration is preferable to UCB's deterministic confidence bounds; any setting
 * where a tunable cooling schedule is convenient.
 *
 * **Randomness:** one `rng.nextDouble()` per arm per evaluation.
 */
class Boltzmann(
    /** Initial temperature; the schedule cools from here. */
    val initialTau: Double = 1.0,
    /** Floor on the temperature so the softmax never collapses to a delta. */
    val minTau: Double = 1e-3,
    /** Cooling decay exponent: `tau(t) = initialTau / t^decay`. `0.0` is fixed-temperature. */
    val decay: Double = 1.0,
    priorMean: Double = 0.0,
    priorWeight: Double = 0.02,
) : BanditPolicy<WeightedMeanResult> {
    init {
        require(initialTau > 0.0) { "initialTau must be positive, got $initialTau" }
        require(minTau > 0.0) { "minTau must be positive, got $minTau" }
        require(decay >= 0.0) { "decay must be non-negative, got $decay" }
    }

    override val arm = MeanArm(priorMean, priorWeight)

    /** Temperature at the 0-based [step] the bandit passes to [evaluate]. */
    internal fun temperature(step: Long): Double {
        if (decay == 0.0) return initialTau
        val round = (step + 1L).coerceAtLeast(1L).toDouble()
        return maxOf(minTau, initialTau / round.pow(decay))
    }

    override fun evaluate(snapshot: WeightedMeanResult, step: Long, rng: Random): Double {
        val logit = (snapshot.mean / temperature(step)).takeIf { it.isFinite() } ?: 0.0
        return logit - ln(-ln(rng.nextDouble()))
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
 * [EpsilonGreedy]. Not a real online-learning
 * choice on its own.
 */
class UniformSelection : BanditPolicy<WeightedVarianceResult> {
    override val arm = NormalArm(priorSquaredDeviations = 0.0)
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
) : SampleCountingPolicy<BernoulliSumResult>({ it.trials }) {
    override val arm = BernoulliArm(priorAlpha, priorBeta)

    override fun createPolicy() = KlUcb(c, tolerance, arm.priorAlpha, arm.priorBeta)

    override fun evaluate(snapshot: BernoulliSumResult, step: Long, rng: Random): Double {
        val n = snapshot.trials
        if (n < 1.0 || totalSamples <= 1.0) return Double.POSITIVE_INFINITY
        val mean = (snapshot.successes / n).coerceIn(0.0, 1.0)
        val bound = (ln(totalSamples) + c * ln(ln(totalSamples).coerceAtLeast(1.0))) / n
        return klBernoulliUpper(mean, bound, tolerance)
    }

    /** Bernoulli KL utilities used by [KlUcb]. */
    companion object {
        /** `sup { q in [p, 1] : KL(p, q) <= bound }` via bisection. */
        internal fun klBernoulliUpper(p: Double, bound: Double, tol: Double): Double {
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
        internal fun klBernoulli(p: Double, q: Double): Double {
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
 * Uses the anytime form (no fixed horizon argument). The arm count `K` in the
 * `t / K` denominator comes from the bandit's [addArm] / [removeArm] calls.
 *
 * Reach for it when minimax regret matters more than asymptotic optimality
 *; adversarial reward distributions, settings where the worst case
 * matters. For Bernoulli rewards specifically, [KlUcb] is asymptotically
 * tighter.
 */
class Moss(
    priorMean: Double = 0.0,
    priorWeight: Double = 0.02,
) : SampleCountingPolicy<WeightedMeanResult>({ it.totalWeights }) {
    override val arm = MeanArm(priorMean, priorWeight)

    override fun createPolicy() = Moss(arm.priorMean, arm.priorWeight)

    override fun evaluate(snapshot: WeightedMeanResult, step: Long, rng: Random): Double {
        val n = snapshot.totalWeights
        if (n < 1.0) return Double.POSITIVE_INFINITY
        val arg = totalSamples / (armCount.coerceAtLeast(1) * n)
        val padding = ln(arg.coerceAtLeast(1.0))
        return snapshot.mean + sqrt(padding / n)
    }
}

/**
 * UCB-V; variance-aware UCB with finite-sample honesty (Audibert, Munos,
 * Szepesvári 2009). Score is
 * `mean + sqrt(2 * V * zeta * ln(t) / n) + 3 * c * zeta * ln(t) / n`,
 * where `V` is the running variance from the [WeightedVarianceResult] snapshot.
 *
 * The third term, a bias correction scaled by [c], keeps the bound honest at
 * finite sample sizes rather than only asymptotically. Reach for it when
 * rewards are not `[0, 1]`-bounded, or when sample sizes per arm stay small
 * (early stopping, expensive arms) and the asymptotic tightness of [KlUcb]
 * doesn't materialise.
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
) : SampleCountingPolicy<WeightedVarianceResult>({ it.totalWeights }) {
    init {
        require(zeta > 0.0) { "zeta must be positive, got $zeta" }
    }
    override val arm = NormalArm(priorMean, priorWeight, priorSquaredDeviations = 0.0)

    override fun createPolicy() = UcbV(zeta, c, arm.priorMean, arm.priorWeight)

    override fun evaluate(snapshot: WeightedVarianceResult, step: Long, rng: Random): Double {
        val n = snapshot.totalWeights
        if (n < 1.0) return Double.POSITIVE_INFINITY
        val v = snapshot.variance.coerceAtLeast(0.0)
        val logT = ln(totalSamples.coerceAtLeast(2.0))
        return snapshot.mean + sqrt(2.0 * v * zeta * logT / n) + 3.0 * c * zeta * logT / n
    }
}
