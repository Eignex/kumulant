package com.eignex.kumulant.bandit

import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.math.VectorView
import com.eignex.kumulant.stat.regression.LinearPosterior
import com.eignex.kumulant.stat.regression.LinearRegressionResult
import kotlin.random.Random

/**
 * Linear contextual bandit: each arm is a [RegressionStat] cloned from [template], scored
 * per round by [posterior] under the incoming context vector. Specialises to:
 *
 *  - **Linear Thompson Sampling**: `BayesianRegressionStat` + `MultivariateGaussian`
 *    posterior — each evaluate draws a fresh weight vector and returns its prediction.
 *  - **Greedy**: any `RegressionStat` + `PointPosterior` with `exploration = 0.0` —
 *    score is the mean prediction `bias + x . weights`.
 *  - **LinUCB-style**: pair a [LinearPosterior] whose `evaluate` returns a confidence-
 *    bound score (e.g. mean + `sqrt(xT Sigma x)` * alpha) over `BayesianRegressionStat`.
 *    The built-in [com.eignex.kumulant.stat.regression.MultivariateGaussian] returns a
 *    sampled prediction (Thompson form); UCB-style scoring is a one-line custom
 *    [LinearPosterior].
 *
 * Each arm's regressor is constructed via `template.create(null)` so per-arm state is
 * independent. [exploration] scales the posterior's exploration parameter; pass `0.0`
 * for pure exploitation (point predictions only).
 */
class LinearContextualBandit<R : LinearRegressionResult>(
    override val nbrArms: Int,
    /** Template regressor; one independent copy is allocated per arm via [RegressionStat.create]. */
    private val template: RegressionStat<R>,
    /** Stateless arm scorer applied to each per-arm snapshot at [choose] time. */
    val posterior: LinearPosterior<R>,
    /** Per-evaluate exploration scale forwarded to the posterior; `0.0` collapses to the point estimate. */
    val exploration: Double = 1.0,
    override val random: Random = Random.Default,
) : ContextualBandit<R> {

    init { require(nbrArms > 0) { "nbrArms must be positive, got $nbrArms" } }

    private val arms: Array<RegressionStat<R>> = Array(nbrArms) { template.create(null) }

    override fun choose(x: VectorView): Int {
        var bestIdx = 0
        var bestScore = Double.NEGATIVE_INFINITY
        for (i in 0 until nbrArms) {
            val score = posterior.evaluate(arms[i].read(0L), x, random, exploration)
            if (score > bestScore) {
                bestScore = score
                bestIdx = i
            }
        }
        return bestIdx
    }

    override fun evaluate(armIndex: Int, x: VectorView): Double =
        posterior.evaluate(arms[armIndex].read(0L), x, random, exploration)

    override fun update(armIndex: Int, x: VectorView, reward: Double, weight: Double) {
        arms[armIndex].update(x, reward, weight)
    }

    override fun snapshot(): List<R> = arms.map { it.read(0L) }

    override fun armResult(armIndex: Int): R = arms[armIndex].read(0L)

    override fun armStat(armIndex: Int): RegressionStat<R> = arms[armIndex]

    override fun merge(others: List<R>) {
        require(others.size == nbrArms) {
            "merge: others.size=${others.size} does not match nbrArms=$nbrArms"
        }
        for (i in 0 until nbrArms) arms[i].merge(others[i])
    }

    override fun reset() {
        for (i in 0 until nbrArms) arms[i] = template.create(null)
    }

    override fun create(random: Random): LinearContextualBandit<R> =
        LinearContextualBandit(nbrArms, template, posterior, exploration, random)
}
