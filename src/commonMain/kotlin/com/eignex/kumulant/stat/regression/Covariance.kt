package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.operation.mapResult
import com.eignex.kumulant.stat.summary.VarianceResult

import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.defaultStreamMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.sqrt

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

/**
 * Online covariance and Pearson correlation between two streams.
 *
 * Derived from [OLS]: the same Chan's parallel algorithm drives accumulation,
 * and [CovarianceResult] is projected from [OLSResult] via [mapResult].
 */
class Covariance(
    mode: StreamMode = defaultStreamMode,
) : PairedStat<CovarianceResult> by OLS(mode).mapResult(
    forward = { ols ->
        val w = ols.totalWeights
        val sxx = ols.x.variance * w
        val syy = ols.y.variance * w
        CovarianceResult(
            totalWeights = w,
            meanX = ols.x.mean,
            meanY = ols.y.mean,
            sxy = ols.slope * sxx,
            sxx = sxx,
            syy = syy,
        )
    },
    reverse = { cov ->
        val w = cov.totalWeights
        val slope = if (cov.sxx > 0.0) cov.sxy / cov.sxx else 0.0
        val varX = if (w > 0.0) cov.sxx / w else 0.0
        val varY = if (w > 0.0) cov.syy / w else 0.0
        OLSResult(
            totalWeights = w,
            slope = slope,
            intercept = cov.meanY - slope * cov.meanX,
            sse = (cov.syy - slope * cov.sxy).coerceAtLeast(0.0),
            x = VarianceResult(cov.meanX, varX),
            y = VarianceResult(cov.meanY, varY),
        )
    }
)
