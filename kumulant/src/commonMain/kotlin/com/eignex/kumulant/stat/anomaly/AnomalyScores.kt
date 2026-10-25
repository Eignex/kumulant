package com.eignex.kumulant.stat.anomaly

import com.eignex.kumulant.core.HasCenterScale
import com.eignex.kumulant.stat.quantile.QuantileResult
import com.eignex.kumulant.stat.quantile.SketchResult
import com.eignex.kumulant.stat.quantile.TDigestResult
import kotlin.math.abs

private const val Z_SCORE_FLOOR: Double = 1e-12

/**
 * Absolute standardized distance `|value - center| / scale` from this snapshot.
 * For a mean and standard deviation this is the absolute z-score. Returns zero
 * when the scale is at most `1e-12` or NaN, including an empty or constant stream.
 */
fun HasCenterScale.absoluteZScore(value: Double): Double {
    val currentScale = scale
    return if (currentScale > Z_SCORE_FLOOR) abs(value - center) / currentScale else 0.0
}

/**
 * Returns `1.0` when [value] strictly exceeds this snapshot's quantile, otherwise `0.0`.
 * A NaN value or quantile scores zero.
 */
fun QuantileResult.exceedanceScore(value: Double): Double = if (value > quantile) 1.0 else 0.0

/**
 * Scores [value] against the quantile precomputed at [probability]: `1.0` above it,
 * otherwise `0.0`. A NaN value or quantile scores zero.
 *
 * @throws IllegalArgumentException if [probability] is absent from [SketchResult.probabilities].
 */
fun SketchResult.exceedanceScore(value: Double, probability: Double): Double =
    exceedanceScore(value, probability, probabilities, quantiles)

/**
 * Scores [value] against the quantile precomputed at [probability]: `1.0` above it,
 * otherwise `0.0`. A NaN value or quantile scores zero.
 *
 * @throws IllegalArgumentException if [probability] is absent from [TDigestResult.probabilities].
 */
fun TDigestResult.exceedanceScore(value: Double, probability: Double): Double =
    exceedanceScore(value, probability, probabilities, quantiles)

private fun exceedanceScore(
    value: Double,
    probability: Double,
    probabilities: DoubleArray,
    quantiles: DoubleArray,
): Double {
    val index = probabilities.indexOfFirst { it == probability }
    require(index >= 0) { "probability=$probability is not configured in this snapshot" }
    return if (value > quantiles[index]) 1.0 else 0.0
}
