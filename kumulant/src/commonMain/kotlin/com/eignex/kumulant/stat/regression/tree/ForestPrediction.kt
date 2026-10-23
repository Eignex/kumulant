package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.Vector
import com.eignex.kumulant.stat.summary.VarianceStat
import kotlin.math.PI
import kotlin.math.abs
import kotlin.math.exp
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
