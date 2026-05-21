package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.core.Result
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.pow

/** Single estimated quantile with the [probability] it targets. */
@Serializable
@SerialName("QuantileResult")
data class QuantileResult(
    /** Probability the [quantile] targets. */
    val probability: Double,
    /** Estimated quantile value at [probability]. */
    val quantile: Double,
) : Result

/** DDSketch snapshot: logarithmic bins plus precomputed [quantiles] for [probabilities]. */
@Serializable
@SerialName("SketchResult")
data class SketchResult(
    /** Probabilities at which [quantiles] are evaluated; parallel to [quantiles]. */
    val probabilities: DoubleArray,
    /** Estimated quantile values, parallel to [probabilities]. */
    val quantiles: DoubleArray,
    /** Multiplicative bin-boundary ratio `(1 + relativeError) / (1 - relativeError)`. */
    val gamma: Double,
    /** Cumulative observation weight folded in. */
    val totalWeights: Double,
    /** Weight observed at the zero bin. */
    val zeroCount: Double,
    /** Positive-side bin counts keyed by signed log-bucket index. */
    val positiveBins: Map<Int, Double>,
    /** Negative-side bin counts keyed by signed log-bucket index. */
    val negativeBins: Map<Int, Double>,
) : Result

/** Histogram as parallel `[lowerBounds, upperBounds)` bucket arrays with [weights]. */
@Serializable
@SerialName("SparseHistogramResult")
data class SparseHistogramResult(
    /** Inclusive lower bound of each bucket; parallel to [upperBounds] and [weights]. */
    val lowerBounds: DoubleArray,
    /** Exclusive upper bound of each bucket; parallel to [lowerBounds] and [weights]. */
    val upperBounds: DoubleArray,
    /** Observed weight per bucket; parallel to [lowerBounds] / [upperBounds]. */
    val weights: DoubleArray,
) : Result

/** Project a [SketchResult] into a [SparseHistogramResult] by expanding its bin indices to bucket boundaries. */
fun SketchResult.toSparseHistogram(): SparseHistogramResult {
    val hasZero = zeroCount > 0.0
    val totalBuckets = negativeBins.size + (if (hasZero) 1 else 0) + positiveBins.size

    val lowers = DoubleArray(totalBuckets)
    val uppers = DoubleArray(totalBuckets)
    val weights = DoubleArray(totalBuckets)

    var cursor = 0

    // Negative bins: most-negative first (descending index) so the output remains ordered low-to-high.
    negativeBins.entries.sortedByDescending { it.key }.forEach { (index, weight) ->
        lowers[cursor] = -(gamma.pow(index))
        uppers[cursor] = -(gamma.pow(index - 1))
        weights[cursor] = weight
        cursor++
    }

    if (hasZero) {
        lowers[cursor] = 0.0
        uppers[cursor] = 0.0
        weights[cursor] = zeroCount
        cursor++
    }

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
    )
}
