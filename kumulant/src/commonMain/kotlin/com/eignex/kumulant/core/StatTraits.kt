package com.eignex.kumulant.core

import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.math.VectorView
import kotlin.math.pow
import kotlin.math.sqrt
import kotlin.time.Duration
import kotlin.time.DurationUnit

/** Result carrying a normalized throughput. */
interface HasRate : Result {
    /** The normalized rate in Events Per Second (Hz). */
    val rate: Double

    /**
     * Rescales the throughput to a specific time duration.
     * Example: rate.per(1.minutes) returns Events Per Minute.
     */
    fun per(duration: Duration): Double = rate * duration.toDouble(
        DurationUnit.SECONDS,
    )
}

/**
 * Result exposing a center estimate and a scale estimate. Consumed by the `band`
 * operator and any other downstream that derives `center ± k * scale` style bands.
 */
interface HasCenterScale : Result {
    /** Center of the distribution (mean, median, level, etc.). */
    val center: Double

    /** Scale of the distribution (standard deviation, MAD, span, etc.). */
    val scale: Double
}

/** Result exposing variance-family quantities derived from [sst] and [totalWeights]. */
interface HasSampleVariance : Result {
    /** Cumulative weight of observations that contributed to this result. */
    val totalWeights: Double

    /** Sum-of-squared-deviations total. */
    val sst: Double get() = variance * totalWeights

    /** Population variance: [sst] / [totalWeights]. */
    val variance: Double get() = if (totalWeights > 0) sst / totalWeights else 0.0

    /** Population standard deviation. */
    val stdDev: Double get() = sqrt(variance)

    /** Unbiased sample variance. */
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
    /** Raw 2nd central moment: `Sum (x - mean)^2 * weight`. */
    val m2: Double get() = sst

    /** Raw 3rd central moment: `Sum (x - mean)^3 * weight`. */
    val m3: Double

    /** Raw 4th central moment: `Sum (x - mean)^4 * weight`. */
    val m4: Double

    /** Biased skewness `(m3 / w) / variance^1.5`. */
    val skewness: Double
        get() {
            val v = variance
            val w = totalWeights
            return if (v > 0 && w > 0) (m3 / w) / v.pow(1.5) else 0.0
        }

    /** Biased excess kurtosis (fourth standardized moment minus 3). */
    val kurtosis: Double
        get() {
            val v = variance
            val w = totalWeights
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
 * Fitted linear model `y = bias + weights . x`. Covers both univariate ([HasSlope])
 * and multivariate regression results behind one surface so consumers can be
 * written generically over "anything with a linear predictor".
 */
interface HasLinearModel : Result {
    /** Fitted weight per feature, indexed by the same `i` as the input `x[i]`. */
    val weights: VectorView

    /** Fitted bias / intercept term. */
    val bias: Double

    /** Number of features in [weights]. */
    val featureSize: Int get() = weights.size

    /** Evaluate the fitted hyperplane at [x]. */
    fun predict(x: VectorView): Double {
        require(x.size == weights.size) { "x.size=${x.size}, expected ${weights.size}" }
        var sum = bias
        for (i in 0 until weights.size) sum += x[i] * weights[i]
        return sum
    }
}

/**
 * Univariate special case: `y = slope * x + intercept`. The general
 * [HasLinearModel] surface is derived from `slope`/`intercept` so univariate
 * regression results compose with any consumer written against
 * [HasLinearModel] without storing redundant fields.
 */
interface HasSlope : HasLinearModel {
    /** Fitted slope coefficient `m`. */
    val slope: Double

    /** Fitted intercept `c`. */
    val intercept: Double

    override val weights: VectorView get() = DenseVector.of(doubleArrayOf(slope))
    override val bias: Double get() = intercept
    override val featureSize: Int get() = 1

    /** Evaluate the fitted line at [x]. */
    fun predict(x: Double): Double = (slope * x) + intercept
}

/**
 * Generic interface for regression error metrics.
 * Extends HasSampleVariance because R^2 requires SST.
 */
interface HasRegression : HasSampleVariance {
    /** Sum-of-squared-errors (residuals). */
    val sse: Double

    /** Sum-of-squares due to regression. */
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
