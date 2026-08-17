package com.eignex.kumulant.bandit.contextual

import com.eignex.kumulant.stat.regression.glm.LinearPosterior
import com.eignex.kumulant.stat.regression.glm.LinearRegressionResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Wire-portable specification for a contextual bandit instance.
 *
 * Materialisation lives in `BanditFactory`.
 *
 * Note: [com.eignex.kumulant.bandit.contextual.Exp4Bandit] is intentionally
 * absent from this hierarchy; its experts are function lambdas with no
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

/**
 * A distance function a [KnnContextualSpec] can name on the wire.
 *
 * Sealed rather than a string against a registry, so an unknown metric is a compile error and the wire
 * format admits only metrics that exist. A caller wanting a metric that is not here passes a function to
 * the [KnnContextualBandit] constructor directly; that path cannot round-trip through the wire format,
 * which is the reason this hierarchy is closed.
 */
@Serializable
sealed interface KnnDistance {

    /** Squared Euclidean distance. Skips the square root, which no ranking needs. */
    @Serializable
    @SerialName("SquaredL2")
    data object SquaredL2 : KnnDistance
}

/** Spec for [KnnContextualBandit]. */
@Serializable
@SerialName("KnnContextual")
data class KnnContextualSpec(
    /** Number of arms in the population. */
    val nbrArms: Int,
    /** Neighbourhood size used for scoring. */
    val k: Int = 5,
    /** Maximum observations retained per arm before FIFO eviction. */
    val maxHistoryPerArm: Int = 1024,
    /** Score assigned to arms with no history yet. */
    val coldStartScore: Double = 1.0,
    /** UCB-style exploration scale. */
    val exploration: Double = 1.0,
    /** Distance metric used to find neighbours. */
    val distance: KnnDistance = KnnDistance.SquaredL2,
) : ContextualBanditSpec
