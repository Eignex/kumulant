package com.eignex.kumulant.bandit.contextual

import com.eignex.kumulant.stat.regression.glm.LinearPosterior
import com.eignex.kumulant.stat.regression.glm.LinearRegressionResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Wire-portable specification for a contextual bandit instance.
 *
 * Materialisation lives in `BanditFactory`.
 */
@Serializable
sealed interface ContextualBanditSpec

/**
 * Spec for [RegressionContextualBandit] with a linear-posterior backbone.
 * The [regression] variant picks one of the three [LinearRegressionResult]-typed
 * regressors; the [posterior] selects the matching scoring rule.
 */
@Serializable
@SerialName("RegressionContextual")
data class RegressionContextualSpec(
    /** Number of arms in the population. */
    val nbrArms: Int,
    /** Per-arm regressor template; cloned for each arm. */
    val regression: LinearRegressionSpec,
    /** Stateless arm scorer applied to each per-arm snapshot. */
    val posterior: LinearPosterior<*>,
    /** Per-evaluate exploration scale forwarded to [posterior]. */
    val exploration: Double = 1.0,
    /** Template for the global pooling regressor; `null` disables pooling. */
    val globalRegression: LinearRegressionSpec? = null,
) : ContextualBanditSpec

/**
 * Wire-portable spec for the three [LinearRegressionResult]-typed regressors that
 * [RegressionContextualBandit] composes with. RegressionTree-based regressors and other
 * non-linear stats are not yet wire-portable; construct them programmatically.
 */
@Serializable
sealed interface LinearRegressionSpec {

    /** Spec for [com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat] with isotropic prior. */
    @Serializable
    @SerialName("BayesianRegression")
    data class Bayesian(
        /** Coefficient dimension. */
        val featureSize: Int,
        /** Isotropic prior variance on every coefficient. */
        val priorVariance: Double = 1.0,
    ) : LinearRegressionSpec

    /** Spec for [com.eignex.kumulant.stat.regression.glm.DiagonalRegressionStat]. */
    @Serializable
    @SerialName("DiagonalRegression")
    data class Diagonal(
        /** Coefficient dimension. */
        val featureSize: Int,
        /** Initial per-coordinate precision. */
        val priorPrecision: Double = 1.0,
        /** Constant learning-rate schedule. */
        val learningRate: Double = 1.0,
    ) : LinearRegressionSpec

    /** Spec for [com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat]. */
    @Serializable
    @SerialName("StochasticRegression")
    data class Stochastic(
        /** Coefficient dimension. */
        val featureSize: Int,
        /** Constant learning-rate schedule. */
        val learningRate: Double = 1e-3,
    ) : LinearRegressionSpec
}
