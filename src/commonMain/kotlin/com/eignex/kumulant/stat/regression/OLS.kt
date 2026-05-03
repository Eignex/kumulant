package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.core.HasLinearModel
import com.eignex.kumulant.core.HasRegression
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.stat.summary.VarianceResult

import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.defaultStreamMode
import com.eignex.kumulant.stream.getValue
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

/**
 * Online Ordinary Least Squares (OLS) linear regression: y = slope * x + intercept.
 *
 * Uses Chan's parallel algorithm for numerically stable online updates and merging.
 * Supports weighted observations.
 */
class OLS(
    override val mode: StreamMode = defaultStreamMode,
) : PairedStat<OLSResult> {

    private val w = mode.newDouble(0.0) // total weights
    private val mx = mode.newDouble(0.0) // mean of x
    private val my = mode.newDouble(0.0) // mean of y
    private val sxx = mode.newDouble(0.0) // sum of squared deviations in x
    private val syy = mode.newDouble(0.0) // sum of squared deviations in y
    private val sxy = mode.newDouble(0.0) // sum of cross-deviations (cov * w)

    val totalWeights: Double by w
    val meanX: Double by mx
    val meanY: Double by my

    /**
     * Update with a new (x, y) observation using Chan's online algorithm.
     *
     * Incremental formula (weight-aware Welford):
     *   sXX += weight * dx * dx * oldW / nextW
     *   sXY += weight * dx * dy * oldW / nextW
     *   sYY += weight * dy * dy * oldW / nextW
     */
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0) return

        val nextW = w.addAndGet(weight)
        val oldW = nextW - weight

        val dx = x - mx.load()
        val dy = y - my.load()

        mx.add(dx * weight / nextW)
        my.add(dy * weight / nextW)

        val factor = weight * oldW / nextW
        sxx.add(dx * dx * factor)
        syy.add(dy * dy * factor)
        sxy.add(dx * dy * factor)
    }

    /**
     * Merge an [OLSResult] from another accumulator using Chan's parallel algorithm.
     *
     * Recovers sXX, sYY, sXY from the result's stored variances and slope.
     */
    override fun merge(values: OLSResult) {
        val w2 = values.totalWeights
        if (w2 <= 0.0) return

        val w1 = w.load()
        val nextW = w1 + w2

        val dx = values.x.mean - mx.load()
        val dy = values.y.mean - my.load()

        val sxx2 = values.x.variance * w2
        val syy2 = values.y.variance * w2
        val sxy2 = values.slope * sxx2 // slope = sxy / sxx  →  sxy = slope * sxx

        val factor = w1 * w2 / nextW
        sxx.add(sxx2 + dx * dx * factor)
        syy.add(syy2 + dy * dy * factor)
        sxy.add(sxy2 + dx * dy * factor)

        mx.add(w2 * dx / nextW)
        my.add(w2 * dy / nextW)
        w.add(w2)
    }

    override fun reset() {
        w.store(0.0)
        mx.store(0.0)
        my.store(0.0)
        sxx.store(0.0)
        syy.store(0.0)
        sxy.store(0.0)
    }

    override fun read(timestampNanos: Long): OLSResult {
        val totalW = w.load()
        val meanX = mx.load()
        val meanY = my.load()
        val ssx = sxx.load()
        val ssy = syy.load()
        val ssxy = sxy.load()

        val slope = if (ssx > 0.0) ssxy / ssx else 0.0
        val intercept = meanY - slope * meanX
        val sse = (ssy - slope * ssxy).coerceAtLeast(0.0)

        return OLSResult(
            totalWeights = totalW,
            slope = slope,
            intercept = intercept,
            sse = sse,
            x = VarianceResult(meanX, if (totalW > 0.0) ssx / totalW else 0.0),
            y = VarianceResult(meanY, if (totalW > 0.0) ssy / totalW else 0.0)
        )
    }

    override fun create(mode: StreamMode?) = OLS(mode ?: this.mode)
}
