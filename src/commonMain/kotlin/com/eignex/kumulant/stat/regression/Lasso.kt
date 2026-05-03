package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.core.HasLinearModel
import com.eignex.kumulant.core.HasRegression
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.operation.mapResult
import com.eignex.kumulant.stat.summary.VarianceResult
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.defaultStreamMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

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

/**
 * Univariate lasso regression (single feature, soft-thresholded slope).
 *
 * Slope is `sign(sxy) * max(0, |sxy| - lambda * w) / sxx` — equivalent to OLS
 * at lambda = 0, exactly zero once lambda dominates the cross-deviation.
 *
 * The raw cross-deviation `sxy` is carried on [LassoResult] so that merging
 * via the result type round-trips losslessly even when the regularized slope
 * has been zeroed by soft-thresholding.
 */
class Lasso(
    val lambda: Double,
    mode: StreamMode = defaultStreamMode,
) : PairedStat<LassoResult> by OLS(mode).mapResult(
    forward = { ols ->
        val w = ols.totalWeights
        val sxx = ols.x.variance * w
        val syy = ols.y.variance * w
        val sxy = ols.slope * sxx
        val threshold = lambda * w
        val shrunk = when {
            sxy > threshold -> sxy - threshold
            sxy < -threshold -> sxy + threshold
            else -> 0.0
        }
        val slope = if (sxx > 0.0) shrunk / sxx else 0.0
        val intercept = ols.y.mean - slope * ols.x.mean
        val sse = (syy - 2.0 * slope * sxy + slope * slope * sxx).coerceAtLeast(0.0)
        LassoResult(
            totalWeights = w,
            lambda = lambda,
            slope = slope,
            intercept = intercept,
            sse = sse,
            sxy = sxy,
            x = ols.x,
            y = ols.y,
        )
    },
    reverse = { lasso ->
        val w = lasso.totalWeights
        val sxx = lasso.x.variance * w
        val syy = lasso.y.variance * w
        val olsSlope = if (sxx > 0.0) lasso.sxy / sxx else 0.0
        val olsIntercept = lasso.y.mean - olsSlope * lasso.x.mean
        val olsSse = (syy - olsSlope * lasso.sxy).coerceAtLeast(0.0)
        OLSResult(
            totalWeights = w,
            slope = olsSlope,
            intercept = olsIntercept,
            sse = olsSse,
            x = lasso.x,
            y = lasso.y,
        )
    }
)
