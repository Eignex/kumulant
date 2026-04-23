package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.concurrent.getValue
import com.eignex.kumulant.core.CovarianceResult
import com.eignex.kumulant.core.OLSResult
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.VarianceResult
import com.eignex.kumulant.operation.mapResult

/**
 * Online Ordinary Least Squares (OLS) linear regression: y = slope * x + intercept.
 *
 * Uses Chan's parallel algorithm for numerically stable online updates and merging.
 * Supports weighted observations.
 *
 * Thread safety follows the chosen [mode]; use [com.eignex.kumulant.concurrent.AtomicMode]
 * or [com.eignex.kumulant.concurrent.FixedAtomicMode] for concurrent single-pass updates.
 */
class OLS(
    val mode: StreamMode = defaultStreamMode,
) : PairedStat<OLSResult> {

    private val _w = mode.newDouble(0.0) // total weights
    private val _mx = mode.newDouble(0.0) // mean of x
    private val _my = mode.newDouble(0.0) // mean of y
    private val _sxx = mode.newDouble(0.0) // sum of squared deviations in x
    private val _syy = mode.newDouble(0.0) // sum of squared deviations in y
    private val _sxy = mode.newDouble(0.0) // sum of cross-deviations (cov * w)

    val totalWeights: Double by _w
    val meanX: Double by _mx
    val meanY: Double by _my

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

        val nextW = _w.addAndGet(weight)
        val oldW = nextW - weight

        val dx = x - _mx.load()
        val dy = y - _my.load()

        _mx.add(dx * weight / nextW)
        _my.add(dy * weight / nextW)

        val factor = weight * oldW / nextW
        _sxx.add(dx * dx * factor)
        _syy.add(dy * dy * factor)
        _sxy.add(dx * dy * factor)
    }

    /**
     * Merge an [OLSResult] from another accumulator using Chan's parallel algorithm.
     *
     * Recovers sXX, sYY, sXY from the result's stored variances and slope.
     */
    override fun merge(values: OLSResult) {
        val w2 = values.totalWeights
        if (w2 <= 0.0) return

        val w1 = _w.load()
        val nextW = w1 + w2

        val dx = values.x.mean - _mx.load()
        val dy = values.y.mean - _my.load()

        val sxx2 = values.x.variance * w2
        val syy2 = values.y.variance * w2
        val sxy2 = values.slope * sxx2 // slope = sxy / sxx  →  sxy = slope * sxx

        val factor = w1 * w2 / nextW
        _sxx.add(sxx2 + dx * dx * factor)
        _syy.add(syy2 + dy * dy * factor)
        _sxy.add(sxy2 + dx * dy * factor)

        _mx.add(w2 * dx / nextW)
        _my.add(w2 * dy / nextW)
        _w.add(w2)
    }

    override fun reset() {
        _w.store(0.0)
        _mx.store(0.0)
        _my.store(0.0)
        _sxx.store(0.0)
        _syy.store(0.0)
        _sxy.store(0.0)
    }

    override fun read(timestampNanos: Long): OLSResult {
        val w = _w.load()
        val mx = _mx.load()
        val my = _my.load()
        val sxx = _sxx.load()
        val syy = _syy.load()
        val sxy = _sxy.load()

        val slope = if (sxx > 0.0) sxy / sxx else 0.0
        val intercept = my - slope * mx
        val sse = (syy - slope * sxy).coerceAtLeast(0.0)

        return OLSResult(
            totalWeights = w,
            slope = slope,
            intercept = intercept,
            sse = sse,
            x = VarianceResult(mx, if (w > 0.0) sxx / w else 0.0),
            y = VarianceResult(my, if (w > 0.0) syy / w else 0.0)
        )
    }

    override fun create(mode: StreamMode?) = OLS(mode ?: this.mode)
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

