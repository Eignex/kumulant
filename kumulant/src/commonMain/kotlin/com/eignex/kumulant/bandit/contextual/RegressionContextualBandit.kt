package com.eignex.kumulant.bandit.contextual

import com.eignex.kumulant.bandit.ContextualBandit
import com.eignex.kumulant.bandit.ContextualScorable
import com.eignex.kumulant.bandit.PerArmBandit
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.math.VectorView
import com.eignex.kumulant.stat.regression.RegressionPosterior
import kotlin.random.Random

/**
 * Generic contextual bandit: each arm owns a [RegressionStat] cloned from
 * [template] and is scored at choose time by the shared [posterior] under
 * the round's context vector, argmaxed across arms. The same machinery
 * covers every regressor in kumulant:
 *
 *  - **Linear Thompson Sampling**: [com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat]
 *    + [com.eignex.kumulant.stat.regression.glm.MultivariateGaussian].
 *  - **LinUcb**: any linear regressor + [com.eignex.kumulant.stat.regression.glm.LinUcb].
 *  - **Greedy SGD**: [com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat] +
 *    [com.eignex.kumulant.stat.regression.glm.PointPosterior] with `exploration = 0.0`.
 *  - **Decision-tree bandit**: [com.eignex.kumulant.stat.regression.tree.DecisionTreeRegressionStat]
 *    + a [com.eignex.kumulant.stat.regression.tree.TreePosterior].
 *  - **Random-forest bandit**: [com.eignex.kumulant.stat.regression.tree.RandomForestRegressionStat]
 *    + a [com.eignex.kumulant.stat.regression.tree.ForestPosterior].
 *
 * Per-arm regressors are constructed via `template.create(null)` so per-arm
 * state is independent. [exploration] scales the posterior's exploration
 * parameter; pass `0.0` for pure exploitation (point estimates only).
 *
 * Optional continuous pooling: when [globalTemplate] is non-null the bandit
 * also maintains a global regressor that absorbs every `(x, reward)`
 * regardless of arm. Per-arm regressors then fit *residuals* against the
 * global's mean prediction, and arm scoring adds the global's mean back in.
 * The global's mean is read via
 * `posterior.evaluate(globalSnapshot, x, rng, exploration = 0.0)`; i.e. the
 * same posterior at zero exploration; so any regressor whose posterior
 * implements `exploration = 0` as mean-prediction (every built-in one does)
 * can be pooled. Caveats are the same as the linear-only version:
 * policy-weighted global bias, approximate joint fit, exploration variance
 * underestimated where the global itself is uncertain. For true hierarchical
 * Bayes use
 * [com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat.fitPopulationPrior].
 *
 * **Use cases:** parametric and tree-based contextual bandits over scalar
 * rewards; any [RegressionStat] + [RegressionPosterior] pairing that
 * supports the policy you want (Thompson, LinUCB, greedy, tree, forest).
 *
 * **Arms:** contextual with caller-defined feature dimension; `nbrArms`
 * fixed at construction. Per-arm state is the cloned regressor; optional
 * global regressor is a single additional cell.
 *
 * **Memory:** O(nbrArms · regressor-state) plus optional O(regressor-state)
 * for the global. The dominant per-arm term depends on the regressor; e.g.
 * `O(featureSize^2)` for Bayesian/LinUCB Gram matrices, `O(featureSize)` for
 * SGD, tree-size-dependent for trees and forests.
 *
 * **Choose:** O(nbrArms · posterior-evaluate) plus one global evaluate when
 * pooling is on. `posterior-evaluate` is regressor-dependent (e.g.
 * `O(featureSize^2)` for Bayesian sampling, `O(featureSize)` for point
 * predictions).
 *
 * **Update:** O(regressor-update) on the played arm, plus one global update
 * when pooling is on. Regressor-dependent: e.g. `O(featureSize^2)` for
 * Sherman-Morrison Bayesian updates, `O(featureSize)` for SGD.
 *
 * **Randomness:** every posterior evaluate (per arm during `choose`, plus
 * the optional global at `exploration = 0`) receives the caller-supplied
 * [random]; reproducible under a fixed seed if the posterior is.
 *
 * **Concurrency:** per-arm [RegressionStat] carries its own concurrency,
 * and the optional global is a single shared [RegressionStat] whose
 * concurrency it likewise inherits. Cross-arm snapshot consistency during
 * `choose` is best-effort under racing updates.
 */
class RegressionContextualBandit<R : Result>(
    override val nbrArms: Int,
    /** Template regressor; one independent copy is allocated per arm via [RegressionStat.create]. */
    private val template: RegressionStat<R>,
    /** Stateless arm scorer applied to each per-arm snapshot at `choose` time. */
    val posterior: RegressionPosterior<R>,
    /** Per-evaluate exploration scale forwarded to the posterior; `0.0` collapses to the point estimate. */
    val exploration: Double = 1.0,
    /** Template for the global pooling regressor; `null` disables pooling. */
    private val globalTemplate: RegressionStat<R>? = null,
    override val random: Random = Random.Default,
) : ContextualBandit,
    PerArmBandit<R>,
    ContextualScorable {

    init {
        require(nbrArms > 0) { "nbrArms must be positive, got $nbrArms" }
    }

    private val arms: Array<RegressionStat<R>> = Array(nbrArms) { template.create(null) }
    private val global: RegressionStat<R>? = globalTemplate?.create(null)

    private fun globalMean(x: VectorView): Double =
        global?.let { posterior.evaluate(it.read(0L), x, random, 0.0) } ?: 0.0

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
            val gMean = posterior.evaluate(g.read(0L), x, random, 0.0)
            arms[armIndex].update(x, reward - gMean, weight)
            g.update(x, reward, weight)
        }
    }

    override fun snapshot(): List<R> = arms.map { it.read(0L) }

    override fun armResult(armIndex: Int): R = arms[armIndex].read(0L)

    /**
     * Live per-arm regressor. When pooling is on this fits *residuals against the global
     * mean*, so its predictions are deltas, not full predictions; use [evaluate] for
     * the combined score and [globalSnapshot] for the global's state.
     */
    fun armStat(armIndex: Int): RegressionStat<R> = arms[armIndex]

    /** Current global pooling snapshot, or `null` if pooling is disabled. */
    fun globalSnapshot(): R? = global?.read(0L)

    /** Live global pooling regressor, or `null` if pooling is disabled. */
    fun globalStat(): RegressionStat<R>? = global

    override fun merge(other: List<R>) {
        require(other.size == nbrArms) {
            "merge: other.size=${other.size} does not match nbrArms=$nbrArms"
        }
        for (i in 0 until nbrArms) arms[i].merge(other[i])
    }

    /** Merge another bandit replica's global snapshot. No-op when pooling is disabled. */
    fun mergeGlobal(other: R) {
        global?.merge(other)
    }

    override fun reset() {
        for (i in 0 until nbrArms) arms[i] = template.create(null)
        global?.reset()
    }

    override fun create(random: Random): RegressionContextualBandit<R> =
        RegressionContextualBandit(nbrArms, template, posterior, exploration, globalTemplate, random)
}
