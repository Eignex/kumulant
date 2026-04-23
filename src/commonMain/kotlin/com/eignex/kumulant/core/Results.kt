package com.eignex.kumulant.core

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.pow
import kotlin.math.sqrt

@Serializable
sealed interface Result

@Serializable
@SerialName("Count")
data class CountResult(
    val count: Long
) : Result

@Serializable
@SerialName("Sum")
data class SumResult(
    val sum: Double
) : Result

@Serializable
@SerialName("Mean")
data class MeanResult(
    val mean: Double,
) : Result

@Serializable
@SerialName("WeightedMean")
data class WeightedMeanResult(
    val totalWeights: Double,
    val mean: Double,
) : Result

@Serializable
@SerialName("Variance")
data class VarianceResult(
    val mean: Double,
    val variance: Double
) : Result

@Serializable
@SerialName("WeightedVariance")
data class WeightedVarianceResult(
    override val totalWeights: Double,
    val mean: Double,
    override val variance: Double
) : Result, HasSampleVariance

@Serializable
@SerialName("Moments")
data class MomentsResult(
    override val totalWeights: Double,
    val mean: Double,
    override val m2: Double,
    override val m3: Double,
    override val m4: Double
) : Result, HasSampleVariance, HasShapeMoments {
    override val sst: Double get() = m2
}

@Serializable
@SerialName("Min")
data class MinResult(
    val min: Double
) : Result

@Serializable
@SerialName("Max")
data class MaxResult(
    val max: Double
) : Result

@Serializable
@SerialName("Range")
data class RangeResult(
    val min: Double,
    val max: Double
) : Result

@Serializable
@SerialName("Rate")
data class RateResult(
    val startTimestampNanos: Long,
    val totalValue: Double,
    val timestampNanos: Long
) : Result, HasRate {
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
    val timestampNanos: Long,
) : Result, HasRate

@Serializable
@SerialName("DecayingSum")
data class DecayingSumResult(
    val sum: Double,
    val timestampNanos: Long,
) : Result

@Serializable
@SerialName("DecayingMean")
data class DecayingMeanResult(
    val mean: Double,
    /** Effective weight of observations still contributing (decays with time). */
    val totalWeights: Double,
    val timestampNanos: Long,
) : Result

@Serializable
@SerialName("DecayingVariance")
data class DecayingVarianceResult(
    val mean: Double,
    val variance: Double,
    /** Effective weight of observations still contributing (decays with time). */
    val totalWeights: Double,
    val timestampNanos: Long,
) : Result {
    val stdDev: Double get() = sqrt(variance)
}

@Serializable
@SerialName("Quantile")
data class QuantileResult(
    val probability: Double,
    val quantile: Double
) : Result

@Serializable
@SerialName("Sketch")
data class SketchResult(
    val probabilities: DoubleArray,
    val quantiles: DoubleArray,
    val gamma: Double,
    val totalWeights: Double,
    val zeroCount: Double,
    val positiveBins: Map<Int, Double>,
    val negativeBins: Map<Int, Double>
) : Result

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
        weights = weights
    )
}

@Serializable
@SerialName("SparseHistogram")
data class SparseHistogramResult(
    val lowerBounds: DoubleArray,
    val upperBounds: DoubleArray,
    val weights: DoubleArray
) : Result

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

    val covariance: Double
        get() = slope * x.variance
}

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
    val covariance: Double get() = if (totalWeights > 0.0) sxy / totalWeights else 0.0
    val correlation: Double
        get() {
            val denom = sxx * syy
            return if (denom > 0.0) sxy / sqrt(denom) else 0.0
        }
    val varX: Double get() = if (totalWeights > 0.0) sxx / totalWeights else 0.0
    val varY: Double get() = if (totalWeights > 0.0) syy / totalWeights else 0.0
}

/**
 * Ordered list of results with per-entry names. Produced by `ListStats` and the vector
 * expansion helpers.
 *
 * Names disambiguate entries for map-style lookup while preserving positional order.
 * Constructing with duplicate names throws — pass explicit names to disambiguate.
 *
 * Positional producers (e.g. vector-expanded stats) use the secondary constructor
 * which auto-assigns index-based names ("0", "1", ...).
 */
@Serializable
@SerialName("List")
data class ResultList<R : Result>(
    val names: List<String>,
    val results: List<R>,
) : Result {
    init {
        require(names.size == results.size) {
            "names/results size mismatch: ${names.size} vs ${results.size}"
        }
        val duplicates = names.groupingBy { it }.eachCount().filter { it.value > 1 }.keys
        require(duplicates.isEmpty()) {
            "Duplicate names in ResultList: $duplicates"
        }
    }

    /** Positional constructor: auto-assigns index-based names ("0", "1", ...). */
    constructor(results: List<R>) : this(List(results.size) { it.toString() }, results)

    fun toMap(): Map<String, R> = names.zip(results).toMap()
}
