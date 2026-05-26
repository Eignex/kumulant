package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.operation.mapResult
import com.eignex.kumulant.stat.regression.glm.Penalty
import com.eignex.kumulant.stat.regression.glm.UnivariateRegressionResult
import com.eignex.kumulant.stat.regression.glm.UnivariateRegressionStat
import com.eignex.kumulant.stat.summary.VarianceResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.sqrt

/** Weighted covariance snapshot with second-moment sums usable for merging. */
@Serializable
@SerialName("CovarianceResult")
data class CovarianceResult(
    /** Cumulative observation weight folded in. */
    val totalWeights: Double,
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

/**
 * Online covariance and Pearson correlation between two streams.
 *
 * Derived from [UnivariateRegressionStat]: the same Chan's parallel algorithm drives
 * accumulation, and [CovarianceResult] is projected from [UnivariateRegressionResult]
 * via [mapResult].
 *
 * **Use cases:** monitoring joint variability and correlation between two
 * scalar streams (input metric vs output, feature drift detection).
 *
 * **Memory:** O(1) — same as [UnivariateRegressionStat].
 *
 * **Update:** O(1) per paired observation.
 *
 * **Concurrency:** Inherits [UnivariateRegressionStat]'s concurrency model.
 */
class CovarianceStat(concurrency: Concurrency = Concurrency.None) :
    PairedStat<CovarianceResult> by UnivariateRegressionStat(concurrency = concurrency).mapResult(
        forward = { ols ->
            val w = ols.totalWeights
            val sxx = ols.x.variance * w
            val syy = ols.y.variance * w
            CovarianceResult(
                totalWeights = w,
                meanX = ols.x.mean,
                meanY = ols.y.mean,
                sxy = ols.sxy,
                sxx = sxx,
                syy = syy,
            )
        },
        reverse = { cov ->
            val w = cov.totalWeights
            val slope = if (cov.sxx > 0.0) cov.sxy / cov.sxx else 0.0
            val varX = if (w > 0.0) cov.sxx / w else 0.0
            val varY = if (w > 0.0) cov.syy / w else 0.0
            UnivariateRegressionResult(
                penalty = Penalty.None,
                totalWeights = w,
                slope = slope,
                intercept = cov.meanY - slope * cov.meanX,
                sse = (cov.syy - slope * cov.sxy).coerceAtLeast(0.0),
                sxy = cov.sxy,
                x = VarianceResult(cov.meanX, varX),
                y = VarianceResult(cov.meanY, varY),
            )
        },
    )
