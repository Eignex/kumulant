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
 *
 * **Continuous pooling (optional).** If [globalTemplate] is non-null, the bandit also
 * maintains a global regressor that absorbs every `(x, reward)` regardless of arm.
 * Per-arm regressors then fit *residuals* against the global's mean prediction, and
 * arm scoring adds the global's mean back in: `score(a, x) = global_mean(x) +
 * posterior.evaluate(arm_a, x)`. This is the cheap pragmatic shrinkage scheme — cold-
 * start arms aren't dumb because the global already has cross-arm evidence — at ~2x
 * compute per update. Caveats:
 *  - The global is biased by play frequency (the policy oversamples winners), so it's
 *    a "policy-weighted population mean", not a uniform population estimate.
 *  - The per-arm deltas are fit against a moving global, so they're approximate rather
 *    than jointly inferred. Steady-state this washes out; with concept drift it doesn't.
 *  - Exploration in [posterior] applies only to the per-arm delta; the global is
 *    treated as a deterministic mean. Underestimates uncertainty where the global
 *    itself is uncertain (early observations, sparse feature regions).
 *
 * For true hierarchical Bayes use [com.eignex.kumulant.stat.regression.BayesianRegressionStat.fitPopulationPrior]
 * with a periodic refit instead.
 */
class LinearContextualBandit<R : LinearRegressionResult>(
    override val nbrArms: Int,
    /** Template regressor; one independent copy is allocated per arm via [RegressionStat.create]. */
    private val template: RegressionStat<R>,
    /** Stateless arm scorer applied to each per-arm snapshot at [choose] time. */
    val posterior: LinearPosterior<R>,
    /** Per-evaluate exploration scale forwarded to the posterior; `0.0` collapses to the point estimate. */
    val exploration: Double = 1.0,
    /** Template for the global pooling regressor; `null` disables pooling. */
    private val globalTemplate: RegressionStat<R>? = null,
    override val random: Random = Random.Default,
) : ContextualBandit<R> {

    init { require(nbrArms > 0) { "nbrArms must be positive, got $nbrArms" } }

    private val arms: Array<RegressionStat<R>> = Array(nbrArms) { template.create(null) }

    /** Continuous pooling regressor; null when pooling is disabled. */
    private val global: RegressionStat<R>? = globalTemplate?.create(null)

    private fun globalMean(x: VectorView): Double = global?.read(0L)?.linearPredictor(x) ?: 0.0

    override fun choose(x: VectorView): Int {
        val gMean = globalMean(x)
        var bestIdx = 0
        var bestScore = Double.NEGATIVE_INFINITY
        for (i in 0 until nbrArms) {
            val score = gMean + posterior.evaluate(arms[i].read(0L), x, random, exploration)
            if (score > bestScore) {
                bestScore = score
                bestIdx = i
            }
        }
        return bestIdx
    }

    override fun evaluate(armIndex: Int, x: VectorView): Double =
        globalMean(x) + posterior.evaluate(arms[armIndex].read(0L), x, random, exploration)

    override fun update(armIndex: Int, x: VectorView, reward: Double, weight: Double) {
        val g = global
        if (g == null) {
            arms[armIndex].update(x, reward, weight)
        } else {
            val gMean = g.read(0L).linearPredictor(x)
            arms[armIndex].update(x, reward - gMean, weight)
            g.update(x, reward, weight)
        }
    }

    override fun snapshot(): List<R> = arms.map { it.read(0L) }

    override fun armResult(armIndex: Int): R = arms[armIndex].read(0L)

    /**
     * Live per-arm regressor. When pooling is on this fits *residuals against the global
     * mean*, so its predictions are deltas, not full predictions — use [evaluate] for
     * the combined score and [globalSnapshot] for the global's state.
     */
    override fun armStat(armIndex: Int): RegressionStat<R> = arms[armIndex]

    /** Current global pooling snapshot, or `null` if pooling is disabled. */
    fun globalSnapshot(): R? = global?.read(0L)

    /** Live global pooling regressor, or `null` if pooling is disabled. */
    fun globalStat(): RegressionStat<R>? = global

    override fun merge(others: List<R>) {
        require(others.size == nbrArms) {
            "merge: others.size=${others.size} does not match nbrArms=$nbrArms"
        }
        for (i in 0 until nbrArms) arms[i].merge(others[i])
    }

    /** Merge another bandit replica's global snapshot. No-op when pooling is disabled. */
    fun mergeGlobal(other: R) {
        global?.merge(other)
    }

    override fun reset() {
        for (i in 0 until nbrArms) arms[i] = template.create(null)
        global?.reset()
    }

    override fun create(random: Random): LinearContextualBandit<R> =
        LinearContextualBandit(nbrArms, template, posterior, exploration, globalTemplate, random)
}
