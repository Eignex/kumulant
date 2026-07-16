package com.eignex.kumulant.stat.regression.glm

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.DenseVector
import com.eignex.koblas.VectorView
import com.eignex.kumulant.core.HasLinearModel
import com.eignex.kumulant.core.HasRegression
import com.eignex.kumulant.core.Result
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Common shape across multivariate-x linear regression snapshots.
 *
 * Concrete subtypes add uncertainty quantification:
 *  - [StochasticRegressionResult]: point estimates only.
 *  - [DiagonalRegressionResult]: per-coefficient precision (factorised posterior).
 *  - [CovarianceRegressionResult]: full posterior covariance + Cholesky factor.
 *
 * Sealed + `@Serializable`. Concrete weights round-trip as [DenseVector] today;
 * the public field is typed [VectorView] so a sparse variant can swap in without
 * breaking callers. Regression error metrics from [HasRegression] become
 * meaningful once [sse] is tracked; implementations that don't accumulate it
 * return `0.0`.
 */
@Serializable
sealed interface LinearRegressionResult :
    Result,
    HasLinearModel,
    HasRegression {
    override val weights: VectorView

    override val bias: Double

    /** Cumulative observation weight folded in. */
    override val totalWeights: Double

    /** Number of [com.eignex.kumulant.core.RegressionStat.update] calls absorbed; useful
     *  as a bookkeeping counter for learning-rate decay or retraining cadence. */
    val step: Long

    /** Canonical link applied at prediction time; the stored [weights] and [bias] live
     *  in the linear-predictor space, [predict] returns the link-mapped mean. */
    val link: Link

    /** Linear predictor `eta = bias + x . weights`, before the inverse link. */
    fun linearPredictor(x: VectorView): Double {
        require(x.size == weights.size) { "x.size=${x.size}, expected ${weights.size}" }
        var sum = bias
        for (i in 0 until weights.size) sum += x[i] * weights[i]
        return sum
    }

    /** Mean response: `link.invMean(linearPredictor(x))`. For [Link.Identity] this is
     *  the linear predictor itself, matching plain linear regression. */
    override fun predict(x: VectorView): Double = link.invMean(linearPredictor(x))
}

/** SGD weight estimates with no posterior. Cheap, no uncertainty quantification.
 *  [sse] carries the accumulated per-link loss; for [Link.Identity] this is the
 *  classical SSE, for [Link.Logit] / [Link.Log] it is the GLM deviance (negative
 *  log-likelihood). [HasRegression.mse] / [HasRegression.rmse] / [HasRegression.rSquared]
 *  are only natural under Identity. */
@Serializable
@SerialName("StochasticRegressionResult")
data class StochasticRegressionResult(
    override val weights: DenseVector,
    override val bias: Double,
    override val totalWeights: Double,
    override val step: Long,
    override val link: Link = Link.Identity,
    override val sse: Double = 0.0,
    /** Per-optimiser auxiliary state (e.g. Adam's `m`/`v`); empty for plain SGD. */
    val updaterState: List<VectorView> = emptyList(),
) : LinearRegressionResult

/**
 * Factorised posterior: each coefficient has its own precision (= 1/variance) but
 * coefficients are assumed independent. Cheap to maintain and sample from; ignores
 * correlations between features.
 */
@Serializable
@SerialName("DiagonalRegressionResult")
data class DiagonalRegressionResult(
    override val weights: DenseVector,
    override val bias: Double,
    /** Posterior precision (inverse variance) on the bias term. */
    val biasPrecision: Double,
    override val totalWeights: Double,
    override val step: Long,
    /** Per-coefficient precision (inverse variance). Same length as [weights]. */
    val precision: DenseVector,
    override val link: Link = Link.Identity,
    override val sse: Double = 0.0,
) : LinearRegressionResult

/**
 * Full multivariate-Gaussian posterior. Carries the joint covariance and its
 * lower-triangular Cholesky factor L so samplers can draw `w ~ N(mean, cov)` as
 * `mean + L u, u ~ N(0, I)` without redoing the decomposition.
 */
@Serializable
@SerialName("CovarianceRegressionResult")
data class CovarianceRegressionResult(
    override val weights: DenseVector,
    override val bias: Double,
    /** Posterior precision (inverse variance) on the bias term. */
    val biasPrecision: Double,
    override val totalWeights: Double,
    override val step: Long,
    /** Full posterior covariance matrix over [weights]. */
    val covariance: DenseMatrix,
    /** Lower-triangular Cholesky factor of [covariance], maintained in lockstep for sampling. */
    val covarianceL: DenseMatrix,
    override val link: Link = Link.Identity,
    override val sse: Double = 0.0,
) : LinearRegressionResult {
    init {
        require(covariance.rows == weights.size && covarianceL.rows == weights.size) {
            "covariance matrices must be ${weights.size}x${weights.size}"
        }
    }
}
