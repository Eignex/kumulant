package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.Vector
import com.eignex.kumulant.stat.summary.VarianceStat
import kotlin.math.PI
import kotlin.math.abs
import kotlin.math.exp
import kotlin.math.ln
import kotlin.math.max
import kotlin.math.sqrt

/**
 * Ensemble distribution over the mean response at a query point.
 *
 * @property mean Arithmetic mean of member predictions.
 * @property variance Population variance across member predictions, excluding leaf reward noise.
 */
data class ForestPrediction(val mean: Double, val variance: Double)

/** Predict using the distribution of tree means, giving each ensemble member equal weight. */
fun ForestRegressionResult.predictDistribution(features: Vector): ForestPrediction {
    val predictions = VarianceStat()
    for (tree in trees) predictions.update(tree.predict(features), 0L, 1.0)
    val summary = predictions.read(0L)
    return ForestPrediction(summary.mean, summary.variance)
}

/**
 * Gaussian expected improvement over [incumbent]. [maximize] selects reward maximization
 * or cost minimization; [margin] requires additional improvement before receiving credit.
 * Zero variance reduces to deterministic improvement. The normal CDF uses a polynomial
 * approximation; negative rounding error in the final score is clamped to zero.
 */
fun ForestPrediction.expectedImprovement(incumbent: Double, maximize: Boolean = true, margin: Double = 0.0): Double {
    require(variance >= 0.0 && variance.isFinite()) { "variance must be nonnegative and finite" }
    require(mean.isFinite() && incumbent.isFinite()) { "means must be finite" }
    require(margin >= 0.0 && margin.isFinite()) { "margin must be nonnegative and finite" }
    val improvement = (if (maximize) mean - incumbent else incumbent - mean) - margin
    if (variance == 0.0) return max(improvement, 0.0)
    val deviation = sqrt(variance)
    val z = improvement / deviation
    if (z == Double.POSITIVE_INFINITY) return max(improvement, 0.0)
    if (z == Double.NEGATIVE_INFINITY) return 0.0
    val density = exp(-0.5 * z * z) / sqrt(2.0 * PI)
    val t = 1.0 / (1.0 + 0.2316419 * abs(z))
    val tail =
        density * t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))))
    val cdf = if (z >= 0.0) 1.0 - tail else tail
    return max(improvement * cdf + deviation * density, 0.0)
}

/**
 * Expected improvement in positive response units when this distribution models log(response).
 * [incumbent] and [margin] are in original units. The expectation integrates a lognormal
 * variable rather than treating exponentiated Gaussian moments as normally distributed.
 */
fun ForestPrediction.expectedImprovementLogNormal(
    incumbent: Double,
    maximize: Boolean = false,
    margin: Double = 0.0,
): Double {
    require(incumbent > 0.0 && incumbent.isFinite()) { "incumbent must be positive and finite" }
    require(mean.isFinite() && variance >= 0.0 && variance.isFinite()) { "invalid log-response moments" }
    require(margin >= 0.0 && margin.isFinite()) { "margin must be nonnegative and finite" }
    val threshold = if (maximize) incumbent + margin else incumbent - margin
    if (threshold <= 0.0 || !threshold.isFinite()) return 0.0
    if (variance == 0.0) return max(if (maximize) exp(mean) - threshold else threshold - exp(mean), 0.0)
    val deviation = sqrt(variance)
    val z = (ln(threshold) - mean) / deviation
    val expectedLog = mean + variance / 2.0
    val tailCore = ln(threshold) - z * z / 2.0
    return max(
        if (maximize) {
            weightedNormalCdf(deviation - z, expectedLog, tailCore) - threshold * normalCdf(-z)
        } else {
            threshold * normalCdf(z) - weightedNormalCdf(z - deviation, expectedLog, tailCore)
        },
        0.0,
    )
}

private fun normalCdf(value: Double): Double {
    val density = exp(-0.5 * value * value) / sqrt(2.0 * PI)
    val t = 1.0 / (1.0 + 0.2316419 * abs(value))
    val tail = density * t * (
        0.319381530 + t * (
            -0.356563782 + t *
                (1.781477937 + t * (-1.821255978 + t * 1.330274429))
            )
        )
    return if (value >= 0.0) 1.0 - tail else tail
}

private fun weightedNormalCdf(value: Double, logWeight: Double, tailCore: Double): Double {
    if (value >= -8.0) return exp(logWeight + ln(normalCdf(value)))
    val inverseSquare = 1.0 / (value * value)
    val mills = 1.0 + inverseSquare * (-1.0 + inverseSquare * (3.0 + inverseSquare * (-15.0 + 105.0 * inverseSquare)))
    return exp(tailCore - ln(-value) - ln(2.0 * PI) / 2.0 + ln(mills))
}
