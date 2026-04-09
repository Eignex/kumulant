package com.eignex.kumulant.core

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.pow
import kotlin.math.sqrt

@Serializable
sealed interface Result {
    val name: String?
}

val Result.nameOrDefault
    get() = name ?: this::class.simpleName?.removeSuffix("Result") ?: "unknown"

@Serializable
@SerialName("Count")
data class CountResult(
    override val count: Long,
    override val name: String? = null
) : Result, HasCount

@Serializable
@SerialName("Sum")
data class SumResult(
    override val sum: Double,
    override val name: String? = null
) : Result, HasSum

@Serializable
@SerialName("Mean")
data class MeanResult(
    override val mean: Double,
    override val name: String? = null,
) : Result, HasMean

@Serializable
@SerialName("WeightedMean")
data class WeightedMeanResult(
    override val totalWeights: Double,
    override val mean: Double,
    override val name: String? = null,
) : Result, HasTotalWeights, HasMean

@Serializable
@SerialName("Variance")
data class VarianceResult(
    override val mean: Double,
    override val variance: Double,
    override val name: String? = null
) : Result, HasMean, HasVariance

@Serializable
@SerialName("WeightedVariance")
data class WeightedVarianceResult(
    override val totalWeights: Double,
    override val mean: Double,
    override val variance: Double,
    override val name: String? = null
) : Result, HasMean, HasSampleVariance

@Serializable
@SerialName("Moments")
data class MomentsResult(
    override val totalWeights: Double,
    override val mean: Double,
    override val m2: Double,
    override val m3: Double,
    override val m4: Double,
    override val name: String? = null
) : Result, HasTotalWeights, HasMean, HasSampleVariance, HasShapeMoments {
    override val sst: Double get() = m2
}

@Serializable
@SerialName("Min")
data class MinResult(
    override val min: Double,
    override val name: String? = null
) : Result, HasMin

@Serializable
@SerialName("Max")
data class MaxResult(
    override val max: Double,
    override val name: String? = null
) : Result, HasMax

@Serializable
@SerialName("Range")
data class RangeResult(
    override val min: Double,
    override val max: Double,
    override val name: String? = null
) : Result, HasRange

@Serializable
@SerialName("Rate")
data class RateResult(
    val startTimestampNanos: Long,
    val totalValue: Double,
    override val timestampNanos: Long,
    override val name: String? = null
) : Result, HasRate, HasTimestamp {
    override val rate: Double
        get() {
            val durationSeconds = (timestampNanos - startTimestampNanos) / 1e9
            val safeDuration =
                if (durationSeconds <= 0.0) 1e-9 else durationSeconds
            return totalValue / safeDuration
        }
}

@Serializable
@SerialName("DecayingRate")
data class DecayingRateResult(
    override val rate: Double,
    override val timestampNanos: Long,
    override val name: String? = null,
) : Result, HasRate, HasTimestamp

@Serializable
@SerialName("DecayingSum")
data class DecayingSumResult(
    val sum: Double,
    override val timestampNanos: Long,
    override val name: String? = null,
) : Result, HasTimestamp

@Serializable
@SerialName("DecayingMean")
data class DecayingMeanResult(
    override val mean: Double,
    /** Effective weight of observations still contributing (decays with time). */
    val decayingCount: Double,
    override val timestampNanos: Long,
    override val name: String? = null,
) : Result, HasMean, HasTimestamp

@Serializable
@SerialName("DecayingVariance")
data class DecayingVarianceResult(
    override val mean: Double,
    override val variance: Double,
    /** Effective weight of observations still contributing (decays with time). */
    val decayingCount: Double,
    override val timestampNanos: Long,
    override val name: String? = null,
) : Result, HasMean, HasVariance, HasTimestamp {
    val stdDev: Double get() = sqrt(variance)
}

@Serializable
@SerialName("Quantile")
data class QuantileResult(
    override val probability: Double,
    override val quantile: Double,
    override val name: String? = null
) : Result, HasQuantile

@Serializable
@SerialName("Sketch")
data class SketchResult(
    override val probabilities: DoubleArray,
    override val quantiles: DoubleArray,
    val gamma: Double,
    val totalWeights: Double,
    val zeroCount: Double,
    val positiveBins: Map<Int, Double>,
    val negativeBins: Map<Int, Double>,
    override val name: String? = null
) : Result, HasQuantiles

fun SketchResult.toSparseHistogram(): SparseHistogramResult {
    val hasZero = zeroCount > 0.0
    val totalBuckets = negativeBins.size + (if (hasZero) 1 else 0) + positiveBins.size

    val lowers = DoubleArray(totalBuckets)
    val uppers = DoubleArray(totalBuckets)
    val weights = DoubleArray(totalBuckets)

    var cursor = 0

    // 1. Process Negative Bins (Sorted highest index to lowest index -> most negative to closest to zero)
    negativeBins.entries.sortedByDescending { it.key }.forEach { (index, weight) ->
        lowers[cursor] = -(gamma.pow(index))
        uppers[cursor] = -(gamma.pow(index - 1))
        weights[cursor] = weight
        cursor++
    }

    // 2. Process Zero Bin
    if (hasZero) {
        lowers[cursor] = 0.0
        uppers[cursor] = 0.0
        weights[cursor] = zeroCount
        cursor++
    }

    // 3. Process Positive Bins (Sorted lowest index to highest index -> closest to zero to most positive)
    positiveBins.entries.sortedBy { it.key }.forEach { (index, weight) ->
        lowers[cursor] = gamma.pow(index - 1)
        uppers[cursor] = gamma.pow(index)
        weights[cursor] = weight
        cursor++
    }

    return SparseHistogramResult(
        lowerBounds = lowers,
        upperBounds = uppers,
        weights = weights,
        name = name
    )
}

@Serializable
@SerialName("SparseHistogram")
data class SparseHistogramResult(
    override val lowerBounds: DoubleArray,
    override val upperBounds: DoubleArray,
    override val weights: DoubleArray,
    override val name: String? = null
) : Result, HasSparseHistogram

@Serializable
@SerialName("OLS")
data class OLSResult(
    override val totalWeights: Double,
    override val slope: Double,
    override val intercept: Double,
    override val sse: Double,
    val x: VarianceResult,
    val y: VarianceResult,
    override val name: String? = null,
) : Result,
    HasLinearModel,
    HasRegression,
    HasCorrelation,
    HasCovariance {

    /**
     * Calculated from R² and the sign of the slope.
     * This avoids needing to store the raw covariance if not strictly necessary.
     */
    override val correlation: Double
        get() {
            if (sst <= 0.0) return 0.0
            val r2 = (1.0 - (sse / sst)).coerceAtLeast(0.0)
            val r = sqrt(r2)
            return if (slope >= 0) r else -r
        }
    override val sst: Double
        get() = y.variance * totalWeights

    override val covariance: Double
        get() = slope * x.variance
}

@Serializable
@SerialName("Covariance")
data class CovarianceResult(
    override val totalWeights: Double,
    val meanX: Double,
    val meanY: Double,
    /** Sum of cross-deviations: sum((x - meanX)(y - meanY) * w) */
    val sxy: Double,
    /** Sum of squared deviations in x: sum((x - meanX)^2 * w) */
    val sxx: Double,
    /** Sum of squared deviations in y: sum((y - meanY)^2 * w) */
    val syy: Double,
    override val name: String? = null,
) : Result, HasTotalWeights, HasCovariance, HasCorrelation {
    override val covariance: Double get() = if (totalWeights > 0.0) sxy / totalWeights else 0.0
    override val correlation: Double
        get() {
            val denom = sxx * syy
            return if (denom > 0.0) sxy / sqrt(denom) else 0.0
        }
    val varX: Double get() = if (totalWeights > 0.0) sxx / totalWeights else 0.0
    val varY: Double get() = if (totalWeights > 0.0) syy / totalWeights else 0.0
}
