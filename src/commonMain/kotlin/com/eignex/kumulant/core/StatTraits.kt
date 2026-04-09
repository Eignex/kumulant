package com.eignex.kumulant.core

import kotlin.math.pow
import kotlin.math.sqrt
import kotlin.time.Duration
import kotlin.time.DurationUnit

interface HasCount : Result {
    val count: Long
}

interface HasTotalWeights : Result {
    val totalWeights: Double
}

interface HasSum : Result {
    val sum: Double
}

interface HasMin : Result {
    val min: Double
}

interface HasMax : Result {
    val max: Double
}

interface HasRange : HasMin, HasMax

interface HasTimestamp {
    val timestampNanos: Long
}

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

interface HasEntropy : Result {
    val entropy: Double
}

interface HasMean : Result {
    val mean: Double
}

interface HasVariance : Result {
    val variance: Double
}

interface HasSampleVariance : HasTotalWeights {
    /** sum of squares totals */
    val sst: Double get() = variance * totalWeights
    val variance: Double get() = if (totalWeights > 0) sst / totalWeights else 0.0
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

    val sampleStdDev: Double get() = sqrt(sampleVariance)
}

interface HasShapeMoments : HasSampleVariance {
    /** Raw 2nd central moment: sum((x - mean)^2 * weight) */
    val m2: Double get() = sst

    /** Raw 3rd central moment: sum((x - mean)^3 * weight) */
    val m3: Double

    /** Raw 4th central moment: sum((x - mean)^4 * weight) */
    val m4: Double

    val skewness: Double
        get() {
            val v = variance
            val w = totalWeights
            // skew = (m3 / w) / (variance^1.5)
            return if (v > 0 && w > 0) (m3 / w) / v.pow(1.5) else 0.0
        }

    val kurtosis: Double
        get() {
            val v = variance
            val w = totalWeights
            // kurtosis = (m4 / w) / (variance^2) - 3.0
            return if (v > 0 && w > 0) (m4 / w) / v.pow(2.0) - 3.0 else 0.0
        }

    val unbiasedSkewness: Double
        get() {
            if (totalWeights <= 2 || skewness == 0.0) return 0.0
            return (sqrt(totalWeights * (totalWeights - 1)) / (totalWeights - 2)) * skewness
        }

    val unbiasedKurtosis: Double
        get() {
            if (totalWeights <= 3) return 0.0
            val factor1 =
                (totalWeights - 1) / ((totalWeights - 2) * (totalWeights - 3))
            val factor2 = (totalWeights + 1) * kurtosis + 6.0
            return factor1 * factor2
        }
}

interface HasCardinality : Result {
    val cardinality: Long
    val errorBounds: Double
}

interface HasCovariance : Result {
    val covariance: Double
}

interface HasCorrelation : Result {
    val correlation: Double
}

interface HasQuantile : Result {
    val probability: Double
    val quantile: Double
}

interface HasQuantiles : Result {
    val probabilities: DoubleArray
    val quantiles: DoubleArray
}

interface HasHistogram : Result {
    val bounds: DoubleArray
    val weights: DoubleArray
}

interface HasSparseHistogram : Result {
    val lowerBounds: DoubleArray
    val upperBounds: DoubleArray

    val weights: DoubleArray
}

/**
 * Generic interface for model prediction (y = mx + c).
 */
interface HasLinearModel : Result {
    val slope: Double
    val intercept: Double

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

    val mse: Double
        get() = if (totalWeights > 0) sse / totalWeights else 0.0

    val rmse: Double
        get() = sqrt(mse)

    val rSquared: Double
        get() = if (sst > 0) 1.0 - (sse / sst) else 0.0
}
