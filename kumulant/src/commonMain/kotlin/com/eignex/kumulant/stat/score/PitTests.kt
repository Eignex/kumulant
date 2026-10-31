package com.eignex.kumulant.stat.score

import com.eignex.kumulant.core.requirePositiveBins
import com.eignex.kumulant.stat.quantile.SparseHistogramResult
import kotlin.math.abs

/**
 * Pearson chi-squared statistic for uniformity on `[0, 1]`. Compares the
 * empirical bin counts against the uniform expectation `total / numBins` and
 * sums `(observed - expected)^2 / expected` over all [numBins] bins.
 *
 * The histogram is the sparse output of `pitHistogram(numBins)` (or any
 * equal-width BucketHistogramStat over `[0, 1]`); pass the same [numBins] used to
 * configure it so empty bins are accounted for. Underflow / overflow rows are
 * excluded - observations outside `[0, 1]` shouldn't count toward a uniformity
 * test on `[0, 1]`. Returns 0 if total finite weight is non-positive.
 */
fun SparseHistogramResult.pitChiSquared(numBins: Int): Double {
    requirePositiveBins(numBins)
    val width = 1.0 / numBins
    val weightPerBin = DoubleArray(numBins)
    var total = 0.0
    for (i in lowerBounds.indices) {
        if (isBottomEdgeRow(lowerBounds[i], upperBounds[i])) {
            weightPerBin[0] += weights[i]
            total += weights[i]
            continue
        }
        if (!lowerBounds[i].isFinite() || !upperBounds[i].isFinite()) continue
        // Map a finite bin to its slot via its upper bound, with a small ulp guard so the
        // top edge (upperBounds == 1.0) doesn't round into a non-existent bin numBins.
        val idx = ((upperBounds[i] - 1e-12) / width).toInt().coerceIn(0, numBins - 1)
        weightPerBin[idx] += weights[i]
        total += weights[i]
    }
    if (total <= 0.0) return 0.0
    val expected = total / numBins
    var x2 = 0.0
    for (w in weightPerBin) {
        val d = w - expected
        x2 += d * d / expected
    }
    return x2
}

/**
 * Kolmogorov-Smirnov statistic against the uniform distribution on `[0, 1]`.
 * Walks every bin (including empty ones) and returns the supremum of
 * `|empCdf(x) - x|` evaluated at bin upper boundaries.
 *
 * Pass the same [numBins] used to configure `pitHistogram(numBins)`. Underflow /
 * overflow rows are excluded. Returns 0 when total finite weight is non-positive.
 */
fun SparseHistogramResult.pitKsDistance(numBins: Int): Double {
    requirePositiveBins(numBins)
    val width = 1.0 / numBins
    val weightPerBin = DoubleArray(numBins)
    var total = 0.0
    for (i in lowerBounds.indices) {
        if (isBottomEdgeRow(lowerBounds[i], upperBounds[i])) {
            weightPerBin[0] += weights[i]
            total += weights[i]
            continue
        }
        if (!lowerBounds[i].isFinite() || !upperBounds[i].isFinite()) continue
        // Map a finite bin to its slot via its upper bound, with a small ulp guard so the
        // top edge (upperBounds == 1.0) doesn't round into a non-existent bin numBins.
        val idx = ((upperBounds[i] - 1e-12) / width).toInt().coerceIn(0, numBins - 1)
        weightPerBin[idx] += weights[i]
        total += weights[i]
    }
    if (total <= 0.0) return 0.0
    var cum = 0.0
    var ks = 0.0
    for (i in 0 until numBins) {
        cum += weightPerBin[i]
        val empCdf = cum / total
        val uniform = (i + 1).toDouble() / numBins
        val gap = abs(empCdf - uniform)
        if (gap > ks) ks = gap
    }
    return ks
}

// True for the histogram's `(-Inf, 0.0]` underflow row. A PIT of exactly 0.0 is legitimate but
// `pitHistogram` buckets include their upper edge, so it lands there; both tests fold the row into
// the bottom bin rather than skipping it with the other non-finite-bound rows, which would drop
// exactly the over-confident forecaster they exist to catch.
private fun isBottomEdgeRow(lower: Double, upper: Double): Boolean = lower == Double.NEGATIVE_INFINITY && upper == 0.0
