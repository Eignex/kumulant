package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.core.Result
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.pow

/** Single estimated quantile with the [probability] it targets. */
@Serializable
@SerialName("Quantile")
data class QuantileResult(
    val probability: Double,
    val quantile: Double
) : Result

/** DDSketch snapshot: logarithmic bins plus precomputed [quantiles] for [probabilities]. */
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

/** Histogram as parallel `[lowerBounds, upperBounds)` bucket arrays with [weights]. */
@Serializable
@SerialName("SparseHistogram")
data class SparseHistogramResult(
    val lowerBounds: DoubleArray,
    val upperBounds: DoubleArray,
    val weights: DoubleArray
) : Result

/** Project a [SketchResult] into a [SparseHistogramResult] by expanding its bin indices to bucket boundaries. */
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
