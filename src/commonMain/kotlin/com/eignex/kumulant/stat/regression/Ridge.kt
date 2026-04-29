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

/**
 * Univariate ridge regression (single feature, L2-shrunk slope).
 *
 * Slope is `sxy / (sxx + lambda * w)` — equivalent to OLS at lambda = 0,
 * shrinks toward zero as lambda grows. Intercept is the centered closed form
 * `meanY - slope * meanX`. Backed by the same Chan's parallel accumulator as
 * [OLS]; merge round-trips exactly via the [RidgeResult] reverse projection.
 */
class Ridge(
    val lambda: Double,
    mode: StreamMode = defaultStreamMode,
) : PairedStat<RidgeResult> by OLS(mode).mapResult(
    forward = { ols ->
        val w = ols.totalWeights
        val sxx = ols.x.variance * w
        val syy = ols.y.variance * w
        val sxy = ols.slope * sxx
        val denom = sxx + lambda * w
        val slope = if (denom > 0.0) sxy / denom else 0.0
        val intercept = ols.y.mean - slope * ols.x.mean
        val sse = (syy - 2.0 * slope * sxy + slope * slope * sxx).coerceAtLeast(0.0)
        RidgeResult(
            totalWeights = w,
            lambda = lambda,
            slope = slope,
            intercept = intercept,
            sse = sse,
            x = ols.x,
            y = ols.y,
        )
    },
    reverse = { ridge ->
        val w = ridge.totalWeights
        val sxx = ridge.x.variance * w
        val sxy = ridge.slope * (sxx + lambda * w)
        val olsSlope = if (sxx > 0.0) sxy / sxx else 0.0
        val olsIntercept = ridge.y.mean - olsSlope * ridge.x.mean
        val olsSse = (ridge.y.variance * w - olsSlope * sxy).coerceAtLeast(0.0)
        OLSResult(
            totalWeights = w,
            slope = olsSlope,
            intercept = olsIntercept,
            sse = olsSse,
            x = ridge.x,
            y = ridge.y,
        )
    }
)
