package com.eignex.kumulant.bandit

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.math.VectorView
import kotlin.random.Random

/**
 * Context-aware bandit: each round the caller observes a feature vector `x`, calls
 * [choose] to pick an arm, plays it externally, observes a reward, and feeds the
 * `(x, reward)` pair back via [update]. Each arm owns a
 * [com.eignex.kumulant.core.RegressionStat] that predicts reward as a function of `x`;
 * arm selection scores each arm's current model under the new context.
 *
 * Sibling to [UnivariateBandit]; population-state machinery lives on the joint
 * [Bandit] parent. The context-bearing `choose(x)` / `update(armIndex, x, reward)`
 * surface is the only differentiator from the indexless univariate flavour.
 *
 * Implementations source all randomness from [random] so callers control the PRNG.
 */
interface ContextualBandit<R : Result> : Bandit<R> {
    /** Pick an arm to play next, given the per-round context [x]. */
    fun choose(x: VectorView): Int

    /** Score the arm at [armIndex] under the current model and context [x]. Parallels
     *  [UnivariateBandit.evaluate] for the contextual setting. */
    fun evaluate(armIndex: Int, x: VectorView): Double

    /** Fold a single `(x, reward)` observation (with optional [weight]) into the arm at [armIndex]. */
    fun update(armIndex: Int, x: VectorView, reward: Double, weight: Double = 1.0)

    /** Live per-arm sufficient-statistic accumulator at [armIndex]. Returns any [Stat]
     *  flavour appropriate to the implementation: [RegressionContextualBandit] returns
     *  a [com.eignex.kumulant.core.RegressionStat]; tree-based bandits return the
     *  per-arm tree's root [com.eignex.kumulant.core.SeriesStat]. */
    fun armStat(armIndex: Int): Stat<R>

    override fun create(random: Random): ContextualBandit<R>
}
