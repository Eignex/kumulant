package com.eignex.kumulant.core

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.sqrt

/** Fitted Ordinary Least Squares regression with marginal x/y variances for merge. */
@Serializable
@SerialName("OLS")
data class OLSResult(
    override val totalWeights: Double,
    override val slope: Double,
    override val intercept: Double,
    override val sse: Double,
    val x: VarianceResult,
    val y: VarianceResult,
) : Result,
    HasLinearModel,
    HasRegression {

    /**
     * Calculated from R² and the sign of the slope.
     * This avoids needing to store the raw covariance if not strictly necessary.
     */
    val correlation: Double
        get() {
            if (sst <= 0.0) return 0.0
            val r2 = (1.0 - (sse / sst)).coerceAtLeast(0.0)
            val r = sqrt(r2)
            return if (slope >= 0) r else -r
        }
    override val sst: Double
        get() = y.variance * totalWeights

    /** Weighted covariance `slope * var(x)`. */
    val covariance: Double
        get() = slope * x.variance
}

/** Weighted covariance snapshot with second-moment sums usable for merging. */
@Serializable
@SerialName("Covariance")
data class CovarianceResult(
    val totalWeights: Double,
    val meanX: Double,
    val meanY: Double,
    /** Sum of cross-deviations: sum((x - meanX)(y - meanY) * w) */
    val sxy: Double,
    /** Sum of squared deviations in x: sum((x - meanX)^2 * w) */
    val sxx: Double,
    /** Sum of squared deviations in y: sum((y - meanY)^2 * w) */
    val syy: Double,
) : Result {
    /** Sample covariance `sxy / totalWeights`. */
    val covariance: Double get() = if (totalWeights > 0.0) sxy / totalWeights else 0.0

    /** Pearson correlation coefficient. */
    val correlation: Double
        get() {
            val denom = sxx * syy
            return if (denom > 0.0) sxy / sqrt(denom) else 0.0
        }

    /** Population variance of x. */
    val varX: Double get() = if (totalWeights > 0.0) sxx / totalWeights else 0.0

    /** Population variance of y. */
    val varY: Double get() = if (totalWeights > 0.0) syy / totalWeights else 0.0
}

/** Fitted Ridge regression (univariate, L2-shrunk slope). */
@Serializable
@SerialName("Ridge")
data class RidgeResult(
    override val totalWeights: Double,
    val lambda: Double,
    override val slope: Double,
    override val intercept: Double,
    override val sse: Double,
    val x: VarianceResult,
    val y: VarianceResult,
) : Result, HasLinearModel, HasRegression {
    override val sst: Double get() = y.variance * totalWeights
}

/** Fitted Lasso regression (univariate, soft-thresholded slope). */
@Serializable
@SerialName("Lasso")
data class LassoResult(
    override val totalWeights: Double,
    val lambda: Double,
    override val slope: Double,
    override val intercept: Double,
    override val sse: Double,
    /**
     * Raw weighted cross-deviation `sum((x-meanX)(y-meanY)*w)` from the underlying
     * accumulator. Stored explicitly so merge round-trips losslessly even when the
     * regularized [slope] has been zeroed by soft-thresholding.
     */
    val sxy: Double,
    val x: VarianceResult,
    val y: VarianceResult,
) : Result, HasLinearModel, HasRegression {
    override val sst: Double get() = y.variance * totalWeights
}
