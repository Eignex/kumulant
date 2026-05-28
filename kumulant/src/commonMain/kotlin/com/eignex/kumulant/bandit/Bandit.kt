package com.eignex.kumulant.bandit

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.math.VectorView
import kotlin.random.Random

/**
 * Root of every bandit kumulant ships. Carries the bare minimum every flavour
 * needs: arm count, a randomness source, and a way to wipe state back to its
 * prior-seeded baseline.
 *
 * The action and state surfaces are deliberately orthogonal:
 *
 * - **Action surface**; [UnivariateBandit] (indexless arms, `choose()`) and
 *   [ContextualBandit] (per-round context vector, `choose(x)`). Pick the one
 *   that matches the decision shape.
 * - **State surface**; [Snapshotable] (any state shape) with the
 *   [PerArmBandit] convenience for the common case where state is one
 *   [Result] per arm.
 * - **Inspection**; [Scorable] / [ContextualScorable] expose per-arm
 *   scores; bandits whose selection rule is an argmax-over-independent-scores
 *   implement these. Joint-sampling bandits ([com.eignex.kumulant.bandit.univariate.BoltzmannBandit],
 *   [com.eignex.kumulant.bandit.univariate.TopTwoThompsonBandit]) and
 *   exponential-weights bandits ([com.eignex.kumulant.bandit.univariate.Exp3Bandit])
 *   do not; no single per-arm score is meaningful in isolation for them.
 *
 * Each concrete bandit's KDoc states which interfaces it implements and why.
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
     * Fold a single observed reward [value] into the arm at [armIndex] with
     * the given [weight]. Weight is the same observation-weight that runs
     * through the rest of the library; typically `1.0`, occasionally
     * importance-weighted for off-policy correction.
     *
     * Index out of range throws; some bandits also bound-check the value
     * (e.g. Bernoulli arms require `value in {0.0, 1.0}`).
     */
    fun update(armIndex: Int, value: Double, weight: Double = 1.0)

    /**
     * Batched [update]: fold one observation per arm/value pair in a single
     * call. Equivalent to looping [update] but skips per-call overhead and
     * may take a per-bandit lock once.
     *
     * Sizes must match: `armIndices.size == values.size`, and `weights` (if
     * non-null) must also match. A null `weights` argument applies `1.0` to
     * every observation.
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
 * 1. Caller observes `x: VectorView` (e.g. a user feature vector).
 * 2. Caller calls [choose] with `x`; the bandit picks an arm by combining
 *    the per-arm model with the context.
 * 3. Caller plays the arm and observes a reward.
 * 4. Caller calls [update] with the arm index, the **same** context `x`,
 *    and the reward. The bandit updates the per-arm model with the
 *    `(x, reward)` pair.
 *
 * Concrete contextual bandits typically own one [com.eignex.kumulant.core.RegressionStat]
 * per arm ([com.eignex.kumulant.bandit.contextual.RegressionContextualBandit]),
 * one nearest-neighbour reservoir per arm
 * ([com.eignex.kumulant.bandit.contextual.KnnContextualBandit]), or a
 * mixture-of-experts weighting
 * ([com.eignex.kumulant.bandit.contextual.Exp4Bandit]).
 *
 * Implementations source all randomness from [Bandit.random].
 */
interface ContextualBandit : Bandit {
    /**
     * Pick an arm to play next, given the per-round context [x]. The bandit
     * combines the context with its per-arm model to score each arm under a
     * configurable [com.eignex.kumulant.stat.regression.RegressionPosterior]
     * (or analogue) and returns the argmax / sampled choice.
     */
    fun choose(x: VectorView): Int

    /**
     * Fold a single `(x, reward)` observation into the arm at [armIndex].
     * The `weight` is the same observation-weight running through the
     * library; typically `1.0`, occasionally importance-weighted.
     */
    fun update(armIndex: Int, x: VectorView, reward: Double, weight: Double = 1.0)
}

/**
 * State surface for any bandit whose state can be checkpointed, replicated,
 * and merged with a sibling's. Orthogonal to the action surface; every
 * bandit family has its own natural [S]:
 *
 * - Per-arm-stat bandits ([com.eignex.kumulant.bandit.univariate.MultiArmedBandit]
 *   and friends) use `List<R>` where `R` is the per-arm result type; see
 *   the [PerArmBandit] convenience.
 * - Joint-state bandits ([com.eignex.kumulant.bandit.univariate.Exp3Bandit],
 *   [com.eignex.kumulant.bandit.contextual.Exp4Bandit]) use a single state
 *   object that captures the expert weights.
 *
 * The pattern: workers run the same bandit configuration in parallel,
 * periodically call [snapshot], ship snapshots to a coordinator, the
 * coordinator runs its own bandit and folds each worker's snapshot in via
 * [merge]. Because the merge unit is the snapshot (not a live bandit), the
 * worker is free to terminate after each report.
 */
interface Snapshotable<S> {
    /**
     * Materialise the current state as a serialisable snapshot. Reads are
     * non-mutating; call as often as needed without affecting decisions.
     * Same snapshot consistency rules as [com.eignex.kumulant.core.Stat.read]
     *; under [com.eignex.kumulant.core.Concurrency.Relaxed] coupled cells
     * may drift by ULPs.
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
 * Convenience for the dominant case where bandit state is one [Result]
 * per arm. Adds per-arm access on top of [Snapshotable]; useful for
 * inspection, debugging, and policies that want to peek at a single
 * arm's posterior without materialising the whole list.
 *
 * Most univariate bandits implement this; exceptions are
 * [com.eignex.kumulant.bandit.univariate.Exp3Bandit] (state is a weight
 * vector, not per-arm results) and the contextual analogues that don't
 * carry per-arm [Result]s in the strict sense.
 */
interface PerArmBandit<R : Result> : Snapshotable<List<R>> {
    /**
     * Per-arm snapshot at [armIndex]. Default implementation reads from
     * the full [snapshot]; implementations may override to avoid building
     * the entire list when only one arm is needed.
     */
    fun armResult(armIndex: Int): R = snapshot()[armIndex]
}

/**
 * Opt-in per-arm scoring for inspection / debugging / custom selectors.
 * Bandits whose [UnivariateBandit.choose] is an argmax over independent
 * per-arm scores expose this; UCB1, Thompson, epsilon-greedy, etc.
 *
 * Joint-sampling bandits don't implement [Scorable]: their selection rule
 * doesn't decompose into a per-arm score. Boltzmann samples from a softmax
 * over arms (the score of any one arm depends on every other arm's score),
 * Top-Two Thompson samples twice and picks conditionally, Exp3 samples from
 * a weight distribution. For those, use the bandit's [Snapshotable.snapshot]
 * to read the underlying state directly.
 */
interface Scorable {
    /**
     * Score the arm at [armIndex] under the bandit's current state. The
     * value's interpretation is policy-specific; UCB upper bound, Thompson
     * draw, mean estimate, etc.; and what the bandit's `choose` would
     * compare against the other arms' scores.
     */
    fun evaluate(armIndex: Int): Double
}

/**
 * Contextual analog of [Scorable]: per-arm score under the current state
 * and a supplied context vector. Implemented by
 * [com.eignex.kumulant.bandit.contextual.RegressionContextualBandit] and
 * [com.eignex.kumulant.bandit.contextual.KnnContextualBandit]; both have
 * an argmax-shaped selection rule that decomposes into per-arm scores.
 */
interface ContextualScorable {
    /** Score the arm at [armIndex] under the current state and context [x]. */
    fun evaluate(armIndex: Int, x: VectorView): Double
}
