package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.core.HasRegression
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.math.DenseMatrix
import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.math.VectorView
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Common shape across multivariate-x linear regression snapshots.
 *
 * Concrete subtypes add uncertainty quantification:
 *  - [SGDRegressionResult]: point estimates only.
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
sealed interface LinearRegressionResult : Result, HasRegression {
    /** Fitted weight per feature, indexed by the same `i` as the input `x[i]`. */
    val weights: VectorView

    val bias: Double

    /** Cumulative observation weight folded in. */
    override val totalWeights: Double

    /** Number of [com.eignex.kumulant.core.RegressionStat.update] calls absorbed; useful
     *  as a bookkeeping counter for learning-rate decay or retraining cadence. */
    val step: Long

    val featureSize: Int get() = weights.size

    /** Evaluate the fitted hyperplane at [x]. */
    fun predict(x: VectorView): Double {
        require(x.size == weights.size) { "x.size=${x.size}, expected ${weights.size}" }
        var sum = bias
        for (i in 0 until weights.size) sum += x[i] * weights[i]
        return sum
    }
}

/** SGD weight estimates with no posterior. Cheap, no uncertainty quantification. */
@Serializable
@SerialName("SGDRegressionResult")
data class SGDRegressionResult(
    override val weights: DenseVector,
    override val bias: Double,
    override val totalWeights: Double,
    override val step: Long,
    /** Sum of squared residuals (estimated, EMA-style); 0.0 if not tracked. */
    override val sse: Double = 0.0,
    /** Per-optimiser auxiliary state (e.g. Adam's `m`/`v`); empty for plain SGD. */
    val updaterState: List<DenseVector> = emptyList(),
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
    val biasPrecision: Double,
    override val totalWeights: Double,
    override val step: Long,
    /** Per-coefficient precision (inverse variance). Same length as [weights]. */
    val precision: DenseVector,
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
    val biasPrecision: Double,
    override val totalWeights: Double,
    override val step: Long,
    val covariance: DenseMatrix,
    val covarianceL: DenseMatrix,
    override val sse: Double = 0.0,
) : LinearRegressionResult {
    init {
        require(covariance.rows == weights.size && covarianceL.rows == weights.size) {
            "covariance matrices must be ${weights.size}x${weights.size}"
        }
    }
}
