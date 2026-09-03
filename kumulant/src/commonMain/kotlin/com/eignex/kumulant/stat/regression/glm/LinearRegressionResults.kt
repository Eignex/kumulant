@file:OptIn(com.eignex.koblas.UnsafeKoblasApi::class)

package com.eignex.kumulant.stat.regression.glm

import com.eignex.koblas.Workspace
import com.eignex.koblas.core.F64DenseMatrix
import com.eignex.koblas.core.F64DenseVector
import com.eignex.koblas.core.F64VectorLike
import com.eignex.koblas.dense.F64CholeskyDecomposition
import com.eignex.koblas.dense.invert
import com.eignex.koblas.dot
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
 *  - [PrecisionRegressionResult]: full posterior as the Cholesky factor of its precision.
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
        return bias + (x dot weights)
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
 * Full multivariate-Gaussian posterior in square-root information form: the joint uncertainty is
 * carried as the Cholesky factor of the posterior precision `H`, and the covariance is `H⁻¹`.
 *
 * Every consumer reaches the covariance through a triangular solve rather than an inverse. A
 * Thompson draw `w ~ N(mean, S)` is `mean + L⁻ᵀ u` for `u ~ N(0, I)`, because `L⁻ᵀ (L⁻ᵀ)ᵀ = H⁻¹`;
 * a predictive standard deviation `sqrt(xᵀ S x)` is the 2-norm of `L⁻¹ x`. Both are O(n²), the same
 * cost the covariance form paid, and neither needs the n×n inverse materialised.
 */
@Serializable
@SerialName("PrecisionRegressionResult")
data class PrecisionRegressionResult(
    override val weights: F64DenseVector,
    override val bias: Double,
    /** Posterior precision (inverse variance) on the bias term. */
    val biasPrecision: Double,
    override val totalWeights: Double,
    override val step: Long,
    /** Cholesky factor of the posterior precision over [weights]: `H = L·Lᵀ`, covariance `S = H⁻¹`.
     *  Only the lower triangle is meaningful, matching koblas's factor convention. */
    val precisionL: F64DenseMatrix,
    override val link: Link = Link.Identity,
    override val sse: Double = 0.0,
) : LinearRegressionResult {
    init {
        // Check cols too, not just rows: every consumer indexes both dimensions, so a non-square
        // matrix would fail far from here, at the read site.
        require(precisionL.rows == weights.size && precisionL.cols == weights.size) {
            "precisionL must be ${weights.size}x${weights.size}; got ${precisionL.rows}x${precisionL.cols}"
        }
    }

    /**
     * Posterior covariance `S = (L·Lᵀ)⁻¹` materialised from [precisionL]. O(n³) and a fresh n×n
     * matrix per call, so it belongs in reporting and prior fitting; scoring paths should stay on
     * the factor.
     */
    fun covariance(workspace: Workspace? = null): F64DenseMatrix =
        F64CholeskyDecomposition(precisionL).invert(workspace)
}
