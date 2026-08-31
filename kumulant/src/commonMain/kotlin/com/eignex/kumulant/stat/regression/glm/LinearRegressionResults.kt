package com.eignex.kumulant.stat.regression.glm

import com.eignex.koblas.core.F64DenseMatrix
import com.eignex.koblas.core.F64DenseVector
import com.eignex.koblas.core.F64VectorLike
import com.eignex.kumulant.core.HasLinearModel
import com.eignex.kumulant.core.HasRegression
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.requireFeatureSize
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
 * Sealed + `@Serializable`. Concrete weights round-trip as [F64DenseVector] today;
 * the public field is typed [F64VectorLike] so a sparse variant can swap in without
 * breaking callers. Regression error metrics from [HasRegression] become
 * meaningful once [sse] is tracked; implementations that don't accumulate it
 * return `0.0`.
 */
@Serializable
sealed interface LinearRegressionResult :
    Result,
    HasLinearModel,
    HasRegression {
    override val weights: F64VectorLike

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
    fun linearPredictor(x: F64VectorLike): Double {
        x.requireFeatureSize(weights.size)
        var sum = bias
        for (i in 0 until weights.size) sum += x[i] * weights[i]
        return sum
    }

    /** Mean response: `link.invMean(linearPredictor(x))`. For [Link.Identity] this is
     *  the linear predictor itself, matching plain linear regression. */
    override fun predict(x: F64VectorLike): Double = link.invMean(linearPredictor(x))
}

/** SGD weight estimates with no posterior. Cheap, no uncertainty quantification.
 *  [sse] carries the accumulated per-link loss; for [Link.Identity] this is the
 *  classical SSE, for [Link.Logit] / [Link.Log] it is the GLM deviance (negative
 *  log-likelihood). [HasRegression.mse] / [HasRegression.rmse] / [HasRegression.rSquared]
 *  are only natural under Identity. */
@Serializable
@SerialName("StochasticRegressionResult")
data class StochasticRegressionResult(
    override val weights: F64DenseVector,
    override val bias: Double,
    override val totalWeights: Double,
    override val step: Long,
    override val link: Link = Link.Identity,
    override val sse: Double = 0.0,
    /** Per-optimiser auxiliary state (e.g. Adam's `m`/`v`); empty for plain SGD. */
    val updaterState: List<F64VectorLike> = emptyList(),
) : LinearRegressionResult

/**
 * Factorised posterior: each coefficient has its own precision (= 1/variance) but
 * coefficients are assumed independent. Cheap to maintain and sample from; ignores
 * correlations between features.
 */
@Serializable
@SerialName("DiagonalRegressionResult")
data class DiagonalRegressionResult(
    override val weights: F64DenseVector,
    override val bias: Double,
    /** Posterior precision (inverse variance) on the bias term. */
    val biasPrecision: Double,
    override val totalWeights: Double,
    override val step: Long,
    /** Per-coefficient precision (inverse variance). Same length as [weights]. */
    val precision: F64DenseVector,
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
    override val weights: F64DenseVector,
    override val bias: Double,
    /** Posterior precision (inverse variance) on the bias term. */
    val biasPrecision: Double,
    override val totalWeights: Double,
    override val step: Long,
    /** Full posterior covariance matrix over [weights]. */
    val covariance: F64DenseMatrix,
    /** Lower-triangular Cholesky factor of [covariance], maintained in lockstep for sampling. */
    val covarianceL: F64DenseMatrix,
    override val link: Link = Link.Identity,
    override val sse: Double = 0.0,
) : LinearRegressionResult {
    init {
        // Check cols too, not just rows: every consumer indexes both dimensions, so a non-square
        // matrix would fail far from here, at the read site.
        require(
            covariance.rows == weights.size && covariance.cols == weights.size &&
                covarianceL.rows == weights.size && covarianceL.cols == weights.size,
        ) {
            "covariance matrices must be ${weights.size}x${weights.size}; got " +
                "covariance ${covariance.rows}x${covariance.cols} and " +
                "covarianceL ${covarianceL.rows}x${covarianceL.cols}"
        }
    }
}
