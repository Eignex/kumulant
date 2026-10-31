package com.eignex.kumulant.bandit

import com.eignex.koblas.Vector
import com.eignex.kumulant.core.Result
import kotlin.random.Random

/**
 * Shared configuration and reset contract for bandits.
 *
 * [UnivariateBandit] selects an arm without context; [ContextualBandit] selects one
 * for a feature vector. [Snapshotable] adds snapshot, merge, and replica creation;
 * [PerArmBandit] specialises it to one [Result] per arm.
 *
 * [Scorable] and [ContextualScorable] expose independent per-arm scores for inspection
 * or custom selection. Policies that sample a joint arm distribution, such as
 * Top-Two Thompson and EXP3, do not expose independent scores.
 */
interface Bandit {
    /** Number of arms in the population. Fixed at construction; arm indices are `[0, nbrArms)`. */
    val nbrArms: Int

    /**
     * Single source of randomness for [UnivariateBandit.choose] /
     * [ContextualBandit.choose] and any policy-internal sampling. Callers pass
     * a `Random(seed)` at construction for reproducible exploration; the
     * bandit threads the same instance through every randomised decision.
     */
    val random: Random

    /**
     * Clear all state back to the prior-seeded baseline. Equivalent to
     * spawning a fresh bandit with the same configuration via
     * [Snapshotable.create], but in place; keeps the same arm count, policy,
     * concurrency mode, and [random] instance.
     */
    fun reset()
}

/**
 * Online optimizer over a fixed set of unindexed arms. Each round the caller:
 *
 * 1. Calls [choose] to pick an arm.
 * 2. Plays it externally (whatever "playing an arm" means in the application).
 * 3. Observes a reward.
 * 4. Calls [update] with the arm index and the observed reward.
 *
 * The reward type is `Double`; Bernoulli rewards encode as `0.0` / `1.0`,
 * continuous rewards pass through as-is, log-normal rewards may want to be
 * pre-transformed via `ln(value)` before being passed in. Per-arm
 * accumulators interpret the value according to their configured arm type
 * ([com.eignex.kumulant.bandit.univariate.Arm]).
 *
 * Implementations source all randomness from [Bandit.random]; never use
 * `Random.Default` directly so the caller controls the PRNG.
 */
interface UnivariateBandit : Bandit {
    /**
     * Pick an arm to play next. Uses [Bandit.random] for any sampling. The
     * returned index is in `[0, nbrArms)`. Repeated calls without
     * intervening [update]s may return different arms (for randomised
     * selection) or the same arm (for argmax-style policies once the
     * leading arm is well-separated).
     */
    fun choose(): Int

    /**
     * Record reward [value] for [armIndex] with [weight].
     * Arm indices must be in `[0, nbrArms)`. Weight and reward handling follow the
     * implementation's accumulator or policy; see [com.eignex.kumulant.core.Stat]
     * for the shared accumulator weight rules.
     */
    fun update(armIndex: Int, value: Double, weight: Double = 1.0)

    /**
     * Record each arm/reward pair in order by calling [update].
     * [armIndices] and [values] must have equal sizes. Non-null [weights] must have
     * the same size; null applies `1.0` to every observation. Size checks run before
     * any update, but a later update failure can leave earlier observations applied.
     */
    fun updateAll(armIndices: IntArray, values: DoubleArray, weights: DoubleArray? = null) {
        require(armIndices.size == values.size) { "armIndices and values must have equal size" }
        require(weights == null || weights.size == values.size) { "weights must match values size" }
        for (i in armIndices.indices) update(armIndices[i], values[i], weights?.get(i) ?: 1.0)
    }
}

/**
 * Context-aware bandit: each round the caller observes a feature vector,
 * uses it to choose an arm, plays the arm, observes a reward, and feeds the
 * `(context, reward)` pair back to the bandit.
 *
 * The standard contextual lifecycle:
 *
 * 1. Caller observes `x: Vector` (e.g. a user feature vector).
 * 2. Caller calls [choose] with `x`; the bandit picks an arm by combining
 *    the per-arm model with the context.
 * 3. Caller plays the arm and observes a reward.
 * 4. Caller calls [update] with the arm index, the **same** context `x`,
 *    and the reward. The bandit updates the per-arm model with the
 *    `(x, reward)` pair.
 *
 * [com.eignex.kumulant.bandit.contextual.RegressionContextualBandit] owns one
 * [com.eignex.kumulant.core.RegressionStat] per arm.
 *
 * Implementations source all randomness from [Bandit.random].
 */
interface ContextualBandit : Bandit {
    /**
     * Choose an arm for context [x], returning an index in `[0, nbrArms)`.
     * Uses [Bandit.random] for any sampling.
     */
    fun choose(x: Vector): Int

    /**
     * Record [reward] for [armIndex] under context [x] with [weight].
     * Arm indices must be in `[0, nbrArms)`. Pass the context used for [choose];
     * weight and reward handling follow the implementation's model or policy.
     */
    fun update(armIndex: Int, x: Vector, reward: Double, weight: Double = 1.0)
}

/**
 * Snapshot, merge, and replica creation for bandits, independent of their input shape.
 *
 * [S] is the snapshot type. [PerArmBandit] uses `List<R>` with one [Result] per arm,
 * including [com.eignex.kumulant.bandit.univariate.Exp3Bandit]'s exponential weights.
 * Workers can send snapshots to a coordinator without sharing live accumulators.
 * Each implementation documents merge approximations and state omitted from its snapshots.
 */
interface Snapshotable<S> {
    /**
     * Return the current state without adding observations or choosing an arm.
     * Consistency follows the implementation's concurrency contract; a snapshot of
     * independently updated cells need not represent a single instant.
     */
    fun snapshot(): S

    /**
     * Fold another replica's [other] state into this bandit. Most families
     * merge exactly via the underlying stat's parallel-merge formula; SGD-
     * based contextual bandits merge approximately. Each concrete bandit's
     * KDoc documents its merge semantics.
     */
    fun merge(other: S)

    /**
     * Spawn a fresh bandit with the same configuration; state resets to
     * the prior seed. The [random] source is replaced; pass the source
     * you want the new bandit to use for exploration (which is independent
     * of merging in another snapshot's state).
     *
     * Useful when a worker accepts a stream of snapshots to apply
     * sequentially: `create(random).also { it.merge(snapshot) }`.
     */
    fun create(random: Random): Snapshotable<S>
}

/**
 * Bandit state represented as one [Result] per arm, in arm-index order.
 * [armResult] allows inspection of one arm without requiring a full [snapshot].
 */
interface PerArmBandit<R : Result> : Snapshotable<List<R>> {
    /**
     * Per-arm snapshot at [armIndex]. Default implementation reads from
     * the full [snapshot]; implementations may override to avoid building
     * the entire list when only one arm is needed.
     */
    fun armResult(armIndex: Int): R {
        val arms = snapshot()
        requireArmIndex(armIndex, arms.size)
        return arms[armIndex]
    }
}

/**
 * Opt-in per-arm scoring for inspection / debugging / custom selectors.
 * Bandits whose [UnivariateBandit.choose] is an argmax over independent
 * per-arm scores expose this; UCB1, Thompson, epsilon-greedy, etc.
 *
 * Joint-sampling bandits don't implement [Scorable]: their selection rule
 * doesn't decompose into a per-arm score. Top-Two Thompson samples twice and
 * picks conditionally, Exp3 samples from a weight distribution. For those, use
 * the bandit's [Snapshotable.snapshot] to read the underlying state directly.
 */
interface Scorable {
    /**
     * Score [armIndex] under the current state. Higher scores are preferred.
     * The interpretation is policy-specific, such as a confidence bound, posterior draw,
     * or mean estimate. Sampling scores consume the bandit's randomness.
     */
    fun evaluate(armIndex: Int): Double
}

/**
 * Contextual analog of [Scorable]: per-arm score under the current state
 * and a supplied context vector. Implemented by
 * [com.eignex.kumulant.bandit.contextual.RegressionContextualBandit], whose
 * selection rule decomposes into per-arm scores.
 */
interface ContextualScorable {
    /**
     * Score [armIndex] under context [x] and the current state. Higher scores are preferred.
     * The interpretation is policy-specific; sampling scores consume the bandit's randomness.
     */
    fun evaluate(armIndex: Int, x: Vector): Double
}
