package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.Vector
import com.eignex.kumulant.stat.summary.VarianceStat

/**
 * Ensemble distribution over the mean response at a query point.
 *
 * @property mean Arithmetic mean of member predictions.
 * @property variance Population variance across member predictions, excluding leaf reward noise.
 */
data class ForestPrediction(val mean: Double, val variance: Double)

/**
 * Predict using the distribution of tree means, giving each ensemble member equal weight.
 *
 * Trees whose routed leaf holds no positive weight have no prediction and are skipped, as in
 * [mergeForestEvidence]. When every routed leaf is empty the result is `ForestPrediction(0.0, 0.0)`.
 */
fun ForestRegressionResult.predictDistribution(features: Vector): ForestPrediction {
    val predictions = VarianceStat()
    for (tree in trees) {
        val leaf = tree.findLeaf(features)
        if (leaf.totalWeights > 0.0) predictions.update(leaf.mean, 0L, 1.0)
    }
    val summary = predictions.read(0L)
    return ForestPrediction(summary.mean, summary.variance)
}
