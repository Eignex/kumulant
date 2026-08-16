package com.eignex.kumulant.stat.regression.glm

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasRegression
import com.eignex.kumulant.core.HasSlope
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.isNotPositiveWeight
import com.eignex.kumulant.stat.summary.VarianceResult
import com.eignex.kumulant.stream.getValue
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.welfordLock
import com.eignex.kumulant.stream.welfordMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.sqrt

/**
 * Fitted univariate least-squares regression `y = slope * x + intercept`. Carries
 * the marginal `x` / `y` variances and the raw weighted cross-deviation [sxy] so
 * the result round-trips losslessly under merge regardless of [penalty].
 */
@Serializable
@SerialName("UnivariateRegressionResult")
data class UnivariateRegressionResult(
    val penalty: Penalty,
    override val totalWeights: Double,
    override val slope: Double,
    override val intercept: Double,
    override val sse: Double,
    /** Raw weighted cross-deviation `Sum((x - meanX)(y - meanY) * w)` from the underlying
     *  accumulator. Stored explicitly so merge round-trips losslessly even when [slope]
     *  has been soft-thresholded to zero or shrunk away from `sxy / sxx`. */
    val sxy: Double,
    /** Marginal statistics of the `x` stream. */
    val x: VarianceResult,
    /** Marginal statistics of the `y` stream. */
    val y: VarianceResult,
) : Result,
    HasSlope,
    HasRegression {

    override val sst: Double get() = y.variance * totalWeights

    /** Weighted covariance `sxy / totalWeights`. */
    val covariance: Double get() = if (totalWeights > 0.0) sxy / totalWeights else 0.0

    /**
     * Pearson correlation of the `x` and `y` streams, `sxy / sqrt(sxx * syy)`.
     *
     * Computed from the raw cross-deviation rather than from `1 - sse/sst`, because
     * [sse] is evaluated at the penalised [slope]: under [Penalty.L1] with a lambda
     * above the soft-thresholding cut the slope is exactly zero, making `sse == sst`
     * and yielding a correlation of zero for perfectly correlated data. This is a
     * property of the two streams and so is independent of [penalty].
     */
    val correlation: Double
        get() {
            val sxx = x.variance * totalWeights
            val syy = y.variance * totalWeights
            if (sxx <= 0.0 || syy <= 0.0) return 0.0
            return (sxy / sqrt(sxx * syy)).coerceIn(-1.0, 1.0)
        }
}

/**
 * Online univariate linear regression backed by Chan's parallel Welford accumulator
 * on `(x, y)`. A single hot path drives every [Penalty]: accumulation is identical;
 * the penalty's closed-form projection is applied only at [read].
 *
 *  - [Penalty.None]: `slope = sxy / sxx` (ordinary least squares).
 *  - [Penalty.L1]: soft-thresholded `slope = sign(sxy) * max(0, |sxy| - lambda * w) / sxx` (Lasso).
 *  - [Penalty.L2]: `slope = sxy / (sxx + lambda * w)` (Ridge).
 *
 * `intercept = meanY - slope * meanX` in every case. Merging consumes a result type
 * carrying the raw [UnivariateRegressionResult.sxy], so the round trip is exact for
 * every penalty including the L1 case where the regularised slope can be zero.
 *
 * **Use cases:** single-feature streaming regression; calibration of a
 * scalar predictor, dose-response curves, anything where `y ~ slope·x +
 * intercept` covers it. For multi-feature regression reach for
 * [DiagonalRegressionStat] (factorised posterior) or [BayesianRegressionStat]
 * (full posterior).
 *
 * **Memory:** O(1); six doubles plus a lock.
 *
 * **Update:** O(1) per observation.
 *
 * **Concurrency:** Welford-coupled cells. [Concurrency.Strict] and
 * [Concurrency.HighWrite] lock the body; exact match to a serial run up to
 * floating-point reorder ULPs. [Concurrency.Relaxed] drops the lock; the six
 * cells race and coefficients drift ~1e-5 relative under contention but
 * never throw.
 */
class UnivariateRegressionStat(
    /** Regularisation applied at `read()` time; defaults to plain OLS. */
    val penalty: Penalty = Penalty.None,
    override val concurrency: Concurrency = Concurrency.None,
) : PairedStat<UnivariateRegressionResult> {

    private val mode = concurrency.welfordMode()
    private val lock = concurrency.welfordLock()
    private val w = mode.newDouble(0.0)
    private val mx = mode.newDouble(0.0)
    private val my = mode.newDouble(0.0)
    private val sxx = mode.newDouble(0.0)
    private val syy = mode.newDouble(0.0)
    private val sxy = mode.newDouble(0.0)

    /** Live view of the cumulative observation weight. */
    val totalWeights: Double by w

    /** Live view of the running mean of `x`. */
    val meanX: Double by mx

    /** Live view of the running mean of `y`. */
    val meanY: Double by my

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        if (weight.isNotPositiveWeight()) return
        lock.guarded {
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
    }

    override fun merge(values: UnivariateRegressionResult) = lock.guarded {
        val w2 = values.totalWeights
        if (w2 <= 0.0) return@guarded

        val w1 = w.load()
        val nextW = w1 + w2

        val dx = values.x.mean - mx.load()
        val dy = values.y.mean - my.load()

        val sxx2 = values.x.variance * w2
        val syy2 = values.y.variance * w2
        val sxy2 = values.sxy

        val factor = w1 * w2 / nextW
        sxx.add(sxx2 + dx * dx * factor)
        syy.add(syy2 + dy * dy * factor)
        sxy.add(sxy2 + dx * dy * factor)

        mx.add(w2 * dx / nextW)
        my.add(w2 * dy / nextW)
        w.add(w2)
    }

    override fun reset() = lock.guarded {
        w.store(0.0)
        mx.store(0.0)
        my.store(0.0)
        sxx.store(0.0)
        syy.store(0.0)
        sxy.store(0.0)
    }

    override fun read(timestampNanos: Long): UnivariateRegressionResult = lock.guarded {
        val totalW = w.load()
        val meanX = mx.load()
        val meanY = my.load()
        val ssx = sxx.load()
        val ssy = syy.load()
        val ssxy = sxy.load()

        val slope = when (val p = penalty) {
            Penalty.None -> if (ssx > 0.0) ssxy / ssx else 0.0

            is Penalty.L1 -> {
                val threshold = p.lambda * totalW
                val shrunk = when {
                    ssxy > threshold -> ssxy - threshold
                    ssxy < -threshold -> ssxy + threshold
                    else -> 0.0
                }
                if (ssx > 0.0) shrunk / ssx else 0.0
            }

            is Penalty.L2 -> {
                val denom = ssx + p.lambda * totalW
                if (denom > 0.0) ssxy / denom else 0.0
            }
        }
        val intercept = meanY - slope * meanX
        val sse = (ssy - 2.0 * slope * ssxy + slope * slope * ssx).coerceAtLeast(0.0)

        UnivariateRegressionResult(
            penalty = penalty,
            totalWeights = totalW,
            slope = slope,
            intercept = intercept,
            sse = sse,
            sxy = ssxy,
            x = VarianceResult(meanX, if (totalW > 0.0) ssx / totalW else 0.0),
            y = VarianceResult(meanY, if (totalW > 0.0) ssy / totalW else 0.0),
        )
    }

    override fun create(concurrency: Concurrency?) = UnivariateRegressionStat(penalty, concurrency ?: this.concurrency)
}
