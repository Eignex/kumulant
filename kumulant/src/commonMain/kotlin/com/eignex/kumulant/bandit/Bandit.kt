package com.eignex.kumulant.bandit

import com.eignex.kumulant.core.Result
import kotlin.random.Random

/**
 * Shared machinery across every bandit flavour kumulant ships: a fixed population of
 * `R`-typed per-arm accumulators, plus the merge/reset/create plumbing replicas need.
 *
 * Sibling specialisations bolt on flavour-specific `choose`/`update`/`evaluate`:
 *  - [UnivariateBandit] for indexless arms — `choose(): Int`, `update(armIndex, value, weight)`.
 *  - [ContextualBandit] for context-aware arms — `choose(x): Int`, `update(armIndex, x, reward, weight)`.
 *
 * Code that only inspects population state — replica merging, snapshot serialisation,
 * dashboard inspection, persistence — can target `Bandit<R>` and accept either flavour.
 *
 * The standalone weight-vector bandits ([Exp3Bandit], [Exp4Bandit], [BoltzmannBandit],
 * [KnnContextualBandit], [TopTwoThompsonBandit]) deliberately sit *outside* this hierarchy:
 * their state isn't a per-arm `Result`-typed sufficient stat, so the population API
 * here doesn't apply.
 */
interface Bandit<R : Result> {
    /** Number of arms in the population. Fixed at construction. */
    val nbrArms: Int

    /** Single source of randomness for `choose` and any policy-internal sampling. */
    val random: Random

    /** Materialise the current per-arm state for inspection, serialisation, or replica merge. */
    fun snapshot(): List<R>

    /** Per-arm snapshot at [armIndex]; default reads from [snapshot]. Implementations may
     *  override to avoid building the full list when only one arm is needed. */
    fun armResult(armIndex: Int): R = snapshot()[armIndex]

    /** Merge each `others[i]` into the corresponding arm. Length must equal [nbrArms].
     *  Used to combine bandit replicas trained in parallel. */
    fun merge(others: List<R>)

    /** Clear all per-arm state back to the prior-seeded baseline. */
    fun reset()

    /** Spawn a fresh bandit with the same configuration; per-arm state resets to the
     *  prior seed. The [random] source may be replaced (default: this bandit's [random]).
     *
     *  Caveat: bandit policies that carry aggregate state across arms (e.g. UCB1's
     *  `totalSamples`) share that state with the source instance. Pass an independent
     *  policy instance to the constructor if you need a fully isolated bandit. */
    fun create(random: Random = this.random): Bandit<R>
}
