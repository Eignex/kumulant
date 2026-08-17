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
 * equal-width LinearHistogramStat over `[0, 1]`); pass the same [numBins] used to
 * configure it so empty bins are accounted for. Underflow / overflow rows are
 * excluded - observations outside `[0, 1]` shouldn't count toward a uniformity
 * test on `[0, 1]`. Returns 0 if total finite weight is non-positive.
 */
fun SparseHistogramResult.pitChiSquared(numBins: Int): Double {
    requirePositiveBins(numBins)
    var total = 0.0
    var observedFinite = 0
    for (i in lowerBounds.indices) {
        if (lowerBounds[i].isFinite() && upperBounds[i].isFinite()) {
            total += weights[i]
            observedFinite++
        } else if (isTopEdgeRow(lowerBounds[i], upperBounds[i])) {
            total += weights[i]
        }
    }
    if (total <= 0.0) return 0.0
    val expected = total / numBins
    var x2 = 0.0
    var topEdgeWeight = 0.0
    for (i in lowerBounds.indices) {
        if (isTopEdgeRow(lowerBounds[i], upperBounds[i])) topEdgeWeight += weights[i]
    }
    for (i in lowerBounds.indices) {
        if (!lowerBounds[i].isFinite() || !upperBounds[i].isFinite()) continue
        // The top bin absorbs the `[1.0, +Inf)` row, so it is the one bin whose observed weight is
        // not just its own.
        val observed = weights[i] + if (upperBounds[i] == 1.0) topEdgeWeight else 0.0
        val d = observed - expected
        x2 += d * d / expected
    }
    val emptyBins = numBins - observedFinite
    if (emptyBins > 0) {
        // Each empty bin contributes (0 - expected)^2 / expected = expected.
        x2 += emptyBins * expected
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
        if (isTopEdgeRow(lowerBounds[i], upperBounds[i])) {
            weightPerBin[numBins - 1] += weights[i]
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

/**
 * True for the histogram's `[1.0, +Inf)` overflow row.
 *
 * A PIT value of exactly 1.0 is legitimate - the forecast CDF evaluated at the observation can reach
 * its top - but [pitHistogram] is bounded `[0, 1)`, so 1.0 lands in the overflow row. Both tests below
 * skip non-finite-bound rows, which silently dropped those observations from the uniformity statistic
 * and biased both tests toward "calibrated" for exactly the over-confident forecaster they exist to
 * catch. Fold the row into the top bin instead.
 */
private fun isTopEdgeRow(lower: Double, upper: Double): Boolean = lower == 1.0 && upper == Double.POSITIVE_INFINITY
