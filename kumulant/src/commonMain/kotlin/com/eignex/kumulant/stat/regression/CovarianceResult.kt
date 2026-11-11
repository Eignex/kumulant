package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.core.HasObservationCount
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.sqrt

/** Weighted covariance snapshot with second-moment sums usable for merging. */
@Serializable
@SerialName("CovarianceResult")
data class CovarianceResult(
    override val totalWeights: Double,
    /** Weighted running mean of `x`. */
    val meanX: Double,
    /** Weighted running mean of `y`. */
    val meanY: Double,
    /** Sum of cross-deviations: `Sum (x - meanX)(y - meanY) * w`. */
    val sxy: Double,
    /** Sum of squared deviations in x: `Sum (x - meanX)^2 * w`. */
    val sxx: Double,
    /** Sum of squared deviations in y: `Sum (y - meanY)^2 * w`. */
    val syy: Double,
) : HasObservationCount {
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
