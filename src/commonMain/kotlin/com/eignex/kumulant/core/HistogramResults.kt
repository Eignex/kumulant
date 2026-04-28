package com.eignex.kumulant.core

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

/**
 * Reservoir sampling snapshot.
 *
 * [values] holds the retained sample (size up to `capacity`); [keys] holds the
 * parallel A-Res priority keys used to drive merging. [totalSeen] and
 * [totalWeight] count every observed update, not just retained ones.
 */
@Serializable
@SerialName("Reservoir")
data class ReservoirResult(
    val values: DoubleArray,
    val keys: DoubleArray,
    val capacity: Int,
    val totalSeen: Long,
    val totalWeight: Double
) : Result

/** Linear-interpolated quantile at [probability] from a reservoir sample (treats sample as unweighted). */
fun ReservoirResult.quantile(probability: Double): Double {
    require(probability in 0.0..1.0) { "Probability must be between 0.0 and 1.0" }
    if (values.isEmpty()) return Double.NaN
    val sorted = values.copyOf().also { it.sort() }
    if (sorted.size == 1) return sorted[0]
    val rank = probability * (sorted.size - 1)
    val lo = rank.toInt()
    val hi = (lo + 1).coerceAtMost(sorted.size - 1)
    val frac = rank - lo
    return sorted[lo] + frac * (sorted[hi] - sorted[lo])
}

/** Bucket the retained sample into [binCount] equal-width bins between min and max. */
fun ReservoirResult.toSparseHistogram(binCount: Int): SparseHistogramResult {
    require(binCount > 0) { "binCount must be > 0" }
    if (values.isEmpty()) {
        return SparseHistogramResult(DoubleArray(0), DoubleArray(0), DoubleArray(0))
    }
    var lo = values[0]
    var hi = values[0]
    for (v in values) {
        if (v < lo) lo = v
        if (v > hi) hi = v
    }
    if (lo == hi) {
        return SparseHistogramResult(
            doubleArrayOf(lo),
            doubleArrayOf(lo),
            doubleArrayOf(values.size.toDouble())
        )
    }
    val width = (hi - lo) / binCount
    val counts = DoubleArray(binCount)
    for (v in values) {
        val idx = ((v - lo) / width).toInt().coerceIn(0, binCount - 1)
        counts[idx] += 1.0
    }
    var populated = 0
    for (c in counts) if (c > 0.0) populated++
    val lowers = DoubleArray(populated)
    val uppers = DoubleArray(populated)
    val weights = DoubleArray(populated)
    var cursor = 0
    for (i in 0 until binCount) {
        if (counts[i] > 0.0) {
            lowers[cursor] = lo + i * width
            uppers[cursor] = lo + (i + 1) * width
            weights[cursor] = counts[i]
            cursor++
        }
    }
    return SparseHistogramResult(lowers, uppers, weights)
}

/**
 * T-digest snapshot: [means]/[weights] are the centroid arrays sorted by mean,
 * with [quantiles] precomputed for [probabilities] via CDF inversion.
 */
@Serializable
@SerialName("TDigest")
data class TDigestResult(
    val probabilities: DoubleArray,
    val quantiles: DoubleArray,
    val means: DoubleArray,
    val weights: DoubleArray,
    val totalWeight: Double,
    val compression: Double
) : Result

/** Convert centroids to a sparse histogram with bins centered on each centroid. */
fun TDigestResult.toSparseHistogram(): SparseHistogramResult {
    val n = means.size
    if (n == 0) return SparseHistogramResult(DoubleArray(0), DoubleArray(0), DoubleArray(0))
    if (n == 1) {
        return SparseHistogramResult(
            doubleArrayOf(means[0]),
            doubleArrayOf(means[0]),
            doubleArrayOf(weights[0])
        )
    }
    val lowers = DoubleArray(n)
    val uppers = DoubleArray(n)
    for (i in 0 until n) {
        val left = if (i == 0) means[0] else (means[i - 1] + means[i]) / 2.0
        val right = if (i == n - 1) means[n - 1] else (means[i] + means[i + 1]) / 2.0
        lowers[i] = left
        uppers[i] = right
    }
    return SparseHistogramResult(lowers, uppers, weights.copyOf())
}

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
