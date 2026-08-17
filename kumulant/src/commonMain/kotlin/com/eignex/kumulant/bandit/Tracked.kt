package com.eignex.kumulant.bandit

import com.eignex.koblas.DenseVector
import com.eignex.koblas.VectorView
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.requireFeatureSize
import kotlin.random.Random

/**
 * Observability wrapper around any [ContextualBandit]. Every event flows into a
 * small set of aggregate side stats, each modelling a different question about
 * the bandit's behaviour. Arm-level bucketing is a separate stratify op; until
 * that lands, encode the arm into the observation (via the joint template) and
 * read the slope/contrast off the resulting stat.
 *
 * Templates are independent and any subset may be null:
 *
 *  - [chooseTemplate] sees `update(x = context, y = armIndex.toDouble(), weight = 1.0)`
 *    at every `choose`. Models the bandit's *policy*; the distribution of arm
 *    selections as a function of context.
 *  - [updateJointTemplate] sees `update(x = [armIndex.toDouble()] ++ context,
 *    y = reward, weight)` at every `update`. Joint reward model with the chosen
 *    arm prepended as an extra feature; the coefficient on the arm dimension is
 *    the arm-conditional effect. `featureSize` of this template must equal
 *    `1 + contextFeatureSize`.
 *  - [updateMarginalTemplate] sees `update(x = context, y = reward, weight)` at
 *    every `update`. Marginal reward-given-context model, agnostic to arm.
 *    `featureSize` must equal `contextFeatureSize`.
 *  - [updateArmRewardTemplate] sees `update(x = armIndex.toDouble(),
 *    y = reward, weight)` at every `update`. Per-arm reward distribution
 *    expressed as a paired stat; covariance, correlation, or per-arm slope.
 *
 * The wrapper itself only satisfies [ContextualBandit]; the underlying bandit is
 * exposed as [inner] typed `B` so callers reach extra interfaces; `snapshot()`,
 * `armResult`, `evaluate(i, x)`; through `tracked.inner.<method>` without losing
 * static type information.
 */
class TrackedContextualBandit<B : ContextualBandit>(
    /** Underlying bandit; exposed for `PerArmBandit` / `ContextualScorable` access. */
    val inner: B,
    /** Context vector dimension validated against templates and incoming updates. */
    val contextFeatureSize: Int,
    // Plain parameters, not `private val`s: these are read only during construction, so holding them
    // would keep a template regressor alive for the lifetime of every tracked bandit.
    chooseTemplate: RegressionStat<out Result>? = null,
    updateJointTemplate: RegressionStat<out Result>? = null,
    updateMarginalTemplate: RegressionStat<out Result>? = null,
    updateArmRewardTemplate: PairedStat<out Result>? = null,
    private val nowNanos: () -> Long = { 0L },
) : ContextualBandit {

    init {
        require(contextFeatureSize > 0) {
            "contextFeatureSize must be positive, got $contextFeatureSize"
        }
        chooseTemplate?.let {
            require(it.featureSize == contextFeatureSize) {
                "chooseTemplate.featureSize=${it.featureSize}, expected $contextFeatureSize"
            }
        }
        updateJointTemplate?.let {
            require(it.featureSize == contextFeatureSize + 1) {
                "updateJointTemplate.featureSize=${it.featureSize}, expected ${contextFeatureSize + 1} (1 + context)"
            }
        }
        updateMarginalTemplate?.let {
            require(it.featureSize == contextFeatureSize) {
                "updateMarginalTemplate.featureSize=${it.featureSize}, expected $contextFeatureSize"
            }
        }
    }

    override val nbrArms: Int get() = inner.nbrArms
    override val random: Random get() = inner.random

    private val chooseStat: RegressionStat<Result>? =
        @Suppress("UNCHECKED_CAST")
        (chooseTemplate?.create(null) as RegressionStat<Result>?)
    private val updateJointStat: RegressionStat<Result>? =
        @Suppress("UNCHECKED_CAST")
        (updateJointTemplate?.create(null) as RegressionStat<Result>?)
    private val updateMarginalStat: RegressionStat<Result>? =
        @Suppress("UNCHECKED_CAST")
        (updateMarginalTemplate?.create(null) as RegressionStat<Result>?)
    private val updateArmRewardStat: PairedStat<Result>? =
        @Suppress("UNCHECKED_CAST")
        (updateArmRewardTemplate?.create(null) as PairedStat<Result>?)

    override fun choose(x: VectorView): Int {
        x.requireFeatureSize(contextFeatureSize)
        val i = inner.choose(x)
        chooseStat?.update(x, i.toDouble(), nowNanos(), 1.0)
        return i
    }

    override fun update(armIndex: Int, x: VectorView, reward: Double, weight: Double) {
        x.requireFeatureSize(contextFeatureSize)
        inner.update(armIndex, x, reward, weight)
        val ts = nowNanos()
        if (updateJointStat != null) {
            val joint = DoubleArray(contextFeatureSize + 1)
            joint[0] = armIndex.toDouble()
            for (j in 0 until contextFeatureSize) joint[j + 1] = x[j]
            updateJointStat.update(DenseVector.of(joint), reward, ts, weight)
        }
        updateMarginalStat?.update(x, reward, ts, weight)
        updateArmRewardStat?.update(armIndex.toDouble(), reward, ts, weight)
    }

    override fun reset() {
        inner.reset()
        chooseStat?.reset()
        updateJointStat?.reset()
        updateMarginalStat?.reset()
        updateArmRewardStat?.reset()
    }

    /** Snapshot of the policy regressor; null when [chooseTemplate] is unset. */
    fun chooseResult(): Result? = chooseStat?.read(nowNanos())

    /** Snapshot of the joint reward regressor; null when [updateJointTemplate] is unset. */
    fun updateJointResult(): Result? = updateJointStat?.read(nowNanos())

    /** Snapshot of the marginal reward regressor; null when [updateMarginalTemplate] is unset. */
    fun updateMarginalResult(): Result? = updateMarginalStat?.read(nowNanos())

    /** Snapshot of the arm-versus-reward paired stat; null when [updateArmRewardTemplate] is unset. */
    fun updateArmRewardResult(): Result? = updateArmRewardStat?.read(nowNanos())
}

/**
 * Observability wrapper around any [UnivariateBandit]. Univariate has no context
 * vector so two aggregate slots cover the surface:
 *
 *  - [chooseTemplate] sees `update(value = armIndex.toDouble(), weight = 1.0)` at
 *    every `choose`. The arm-pick distribution over time.
 *  - [updateArmRewardTemplate] sees `update(x = armIndex.toDouble(),
 *    y = reward, weight)` at every `update`. Per-arm reward distribution.
 *
 * Both templates are optional; null disables that side.
 */
class TrackedUnivariateBandit<B : UnivariateBandit>(
    /** Underlying bandit; exposed for `PerArmBandit` / `Scorable` access. */
    val inner: B,
    chooseTemplate: SeriesStat<out Result>? = null,
    updateArmRewardTemplate: PairedStat<out Result>? = null,
    private val nowNanos: () -> Long = { 0L },
) : UnivariateBandit {

    override val nbrArms: Int get() = inner.nbrArms
    override val random: Random get() = inner.random

    private val chooseStat: SeriesStat<Result>? =
        @Suppress("UNCHECKED_CAST")
        (chooseTemplate?.create(null) as SeriesStat<Result>?)
    private val updateArmRewardStat: PairedStat<Result>? =
        @Suppress("UNCHECKED_CAST")
        (updateArmRewardTemplate?.create(null) as PairedStat<Result>?)

    override fun choose(): Int {
        val i = inner.choose()
        chooseStat?.update(i.toDouble(), nowNanos(), 1.0)
        return i
    }

    override fun update(armIndex: Int, value: Double, weight: Double) {
        inner.update(armIndex, value, weight)
        updateArmRewardStat?.update(armIndex.toDouble(), value, nowNanos(), weight)
    }

    override fun reset() {
        inner.reset()
        chooseStat?.reset()
        updateArmRewardStat?.reset()
    }

    /** Snapshot of the choose-side arm-pick distribution; null when [chooseTemplate] is unset. */
    fun chooseResult(): Result? = chooseStat?.read(nowNanos())

    /** Snapshot of the arm-versus-reward paired stat; null when [updateArmRewardTemplate] is unset. */
    fun updateArmRewardResult(): Result? = updateArmRewardStat?.read(nowNanos())
}
