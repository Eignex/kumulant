package com.eignex.kumulant.bandit.contextual

import com.eignex.kumulant.stat.regression.FactorisedGaussian
import com.eignex.kumulant.stat.regression.LinUcb
import com.eignex.kumulant.stat.regression.LinearPosterior
import com.eignex.kumulant.stat.regression.LinearRegressionResult
import com.eignex.kumulant.stat.regression.MultivariateGaussian
import com.eignex.kumulant.stat.regression.PointPosterior
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Wire-portable specification for a contextual bandit instance.
 *
 * Materialisation lives in [com.eignex.kumulant.bandit.BanditFactory].
 *
 * Note: [com.eignex.kumulant.bandit.contextual.Exp4Bandit] is intentionally
 * absent from this hierarchy — its experts are function lambdas with no
 * obvious wire representation, so it must be constructed programmatically.
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
    val nbrArms: Int,
    val regression: LinearRegressionSpec,
    val posterior: LinearPosterior<*>,
    val exploration: Double = 1.0,
    val globalRegression: LinearRegressionSpec? = null,
) : ContextualBanditSpec

/**
 * Wire-portable spec for the three [LinearRegressionResult]-typed regressors that
 * [RegressionContextualBandit] composes with. Tree-based regressors and other
 * non-linear stats are not yet wire-portable; construct them programmatically.
 */
@Serializable
sealed interface LinearRegressionSpec {

    /** Spec for [com.eignex.kumulant.stat.regression.BayesianRegressionStat] with isotropic prior. */
    @Serializable
    @SerialName("BayesianRegression")
    data class Bayesian(
        val featureSize: Int,
        val priorVariance: Double = 1.0,
    ) : LinearRegressionSpec

    /** Spec for [com.eignex.kumulant.stat.regression.DiagonalRegressionStat]. */
    @Serializable
    @SerialName("DiagonalRegression")
    data class Diagonal(
        val featureSize: Int,
        val priorPrecision: Double = 1.0,
        val learningRate: Double = 1.0,
    ) : LinearRegressionSpec

    /** Spec for [com.eignex.kumulant.stat.regression.StochasticRegressionStat]. */
    @Serializable
    @SerialName("StochasticRegression")
    data class Stochastic(
        val featureSize: Int,
        val learningRate: Double = 1e-3,
    ) : LinearRegressionSpec
}

/** Spec for [KnnContextualBandit]. [distance] is a named lookup against a
 *  small built-in registry — currently `"squaredL2"` is the only stock entry. */
@Serializable
@SerialName("KnnContextual")
data class KnnContextualSpec(
    val nbrArms: Int,
    val k: Int = 5,
    val maxHistoryPerArm: Int = 1024,
    val coldStartScore: Double = 1.0,
    val exploration: Double = 1.0,
    val distance: String = "squaredL2",
) : ContextualBanditSpec
