package com.eignex.kumulant.core

import kotlin.math.pow
import kotlin.math.sqrt
import kotlin.time.Duration
import kotlin.time.DurationUnit

/** Result carrying a normalized throughput. */
interface HasRate : Result {
    /** The normalized rate in Events Per Second (Hz) */
    val rate: Double

    /**
     * Rescales the throughput to a specific time duration.
     * Example: rate.per(1.minutes) returns Events Per Minute.
     */
    fun per(duration: Duration): Double = rate * duration.toDouble(
        DurationUnit.SECONDS
    )
}

/** Result exposing variance-family quantities derived from [sst] and [totalWeights]. */
interface HasSampleVariance : Result {
    /** Cumulative weight of observations that contributed to this result. */
    val totalWeights: Double

    /** sum of squares totals */
    val sst: Double get() = variance * totalWeights

    /** Population variance: [sst] / [totalWeights]. */
    val variance: Double get() = if (totalWeights > 0) sst / totalWeights else 0.0

    /** Population standard deviation. */
    val stdDev: Double get() = sqrt(variance)

    /** *
     * Unbiased Sample Variance.
     */
    val sampleVariance: Double
        get() = if (totalWeights > 1.0) {
            sst / (totalWeights - 1.0)
        } else {
            0.0
        }

    /** Unbiased sample standard deviation. */
    val sampleStdDev: Double get() = sqrt(sampleVariance)
}

/** Result exposing higher central moments plus skewness and kurtosis. */
interface HasShapeMoments : HasSampleVariance {
    /** Raw 2nd central moment: sum((x - mean)^2 * weight) */
    val m2: Double get() = sst

    /** Raw 3rd central moment: sum((x - mean)^3 * weight) */
    val m3: Double

    /** Raw 4th central moment: sum((x - mean)^4 * weight) */
    val m4: Double

    /** Biased skewness `(m3 / w) / variance^1.5`. */
    val skewness: Double
        get() {
            val v = variance
            val w = totalWeights
            // skew = (m3 / w) / (variance^1.5)
            return if (v > 0 && w > 0) (m3 / w) / v.pow(1.5) else 0.0
        }

    /** Biased excess kurtosis (fourth standardized moment minus 3). */
    val kurtosis: Double
        get() {
            val v = variance
            val w = totalWeights
            // kurtosis = (m4 / w) / (variance^2) - 3.0
            return if (v > 0 && w > 0) (m4 / w) / v.pow(2.0) - 3.0 else 0.0
        }

    /** Sample-size-adjusted (unbiased) skewness. */
    val unbiasedSkewness: Double
        get() {
            if (totalWeights <= 2 || skewness == 0.0) return 0.0
            return (sqrt(totalWeights * (totalWeights - 1)) / (totalWeights - 2)) * skewness
        }

    /** Sample-size-adjusted (unbiased) excess kurtosis. */
    val unbiasedKurtosis: Double
        get() {
            if (totalWeights <= 3) return 0.0
            val factor1 =
                (totalWeights - 1) / ((totalWeights - 2) * (totalWeights - 3))
            val factor2 = (totalWeights + 1) * kurtosis + 6.0
            return factor1 * factor2
        }
}

/**
 * Generic interface for model prediction (y = mx + c).
 */
interface HasLinearModel : Result {
    /** Fitted slope coefficient `m`. */
    val slope: Double

    /** Fitted intercept `c`. */
    val intercept: Double

    /** Evaluate the fitted line at [x]. */
    fun predict(x: Double): Double = (slope * x) + intercept
}

/**
 * Generic interface for regression error metrics.
 * Extends HasSampleVariance because R^2 requires SST.
 */
interface HasRegression : HasSampleVariance {
    /** Sum of Squared Errors (Residuals) */
    val sse: Double

    /** Sum of squares due to regression */
    val ssr: Double get() = sst - sse

    /** Mean squared error. */
    val mse: Double
        get() = if (totalWeights > 0) sse / totalWeights else 0.0

    /** Root mean squared error. */
    val rmse: Double
        get() = sqrt(mse)

    /** Coefficient of determination `1 - sse/sst`. */
    val rSquared: Double
        get() = if (sst > 0) 1.0 - (sse / sst) else 0.0
}
