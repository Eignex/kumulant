package com.eignex.kumulant.bandit

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.math.VectorView
import kotlin.random.Random

/**
 * Context-aware bandit: each round the caller observes a feature vector `x`, calls
 * [choose] to pick an arm, plays it externally, observes a reward, and feeds the
 * `(x, reward)` pair back via [update]. Each arm owns a [com.eignex.kumulant.core.RegressionStat] that
 * predicts reward as a function of `x`; arm selection scores each arm's current
 * model under the new context.
 *
 * Sibling to [UnivariateBandit] (not a subtype): the context-bearing `choose(x)` /
 * `update(armIndex, x, reward)` surface doesn't fit the indexless [UnivariateBandit]
 * contract, so they coexist as parallel abstractions over per-arm accumulators.
 *
 * Implementations source all randomness from [random] so callers control the PRNG
 * (seeded for tests, shared for production, custom for special cases).
 */
interface ContextualBandit<R : Result> {
    /** Number of arms in the population. */
    val nbrArms: Int

    /** Single source of randomness for [choose] and any policy-internal sampling. */
    val random: Random

    /** Pick an arm to play next, given the per-round context [x]. */
    fun choose(x: VectorView): Int

    /** Score the arm at [armIndex] under the current model and context [x]. Parallels
     *  [UnivariateBandit.evaluate] for the contextual setting. */
    fun evaluate(armIndex: Int, x: VectorView): Double

    /** Fold a single `(x, reward)` observation (with optional [weight]) into the arm at [armIndex]. */
    fun update(armIndex: Int, x: VectorView, reward: Double, weight: Double = 1.0)

    /** Materialise the current per-arm regression posteriors. */
    fun snapshot(): List<R>

    /** Per-arm snapshot at [armIndex]; default reads from [snapshot]. Implementations may
     *  override to avoid building the full list when only one arm is needed. */
    fun armResult(armIndex: Int): R = snapshot()[armIndex]

    /** Live per-arm sufficient-statistic accumulator; exposed so callers can compose with
     *  the stat ecosystem - inspect, plug into a [com.eignex.kumulant.schema.StatGroup],
     *  or apply ops via the live-stat extensions. Writes should still flow through
     *  [update] to keep the bandit's bookkeeping in sync.
     *
     *  Implementations are free to return any [Stat] flavour appropriate to their
     *  internal representation: [RegressionContextualBandit] returns a
     *  [com.eignex.kumulant.core.RegressionStat]; tree-based bandits return the
     *  per-arm tree's root [com.eignex.kumulant.core.SeriesStat]. */
    fun armStat(armIndex: Int): Stat<R>

    /** Merge each `others[i]` into the corresponding arm. Length must equal [nbrArms].
     *  Used to combine bandit replicas trained in parallel. */
    fun merge(others: List<R>)

    /** Clear all per-arm state back to the prior-seeded baseline. */
    fun reset()

    /** Spawn a fresh bandit with the same configuration; per-arm state resets to the
     *  prior seed. The [random] source may be replaced (default: this bandit's [random]). */
    fun create(random: Random = this.random): ContextualBandit<R>
}
