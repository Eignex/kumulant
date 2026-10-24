package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.DenseVector
import com.eignex.koblas.Vector
import com.eignex.kumulant.math.nextNormal
import com.eignex.kumulant.stat.summary.MeanEstimate
import com.eignex.kumulant.stat.summary.estimateMean
import kotlin.math.sqrt
import kotlin.random.Random

/** Separate uncertainty components for a randomized regression forest. */
data class ForestMoments(
    /** Mean of the fitted tree predictions. */
    val mean: Double,
    /** Population variance between tree means. */
    val disagreementVariance: Double,
    /** Average leaf-mean sampling variance estimated from original, unbootstrapped observations. */
    val leafMeanVariance: Double,
    /** Average within-leaf response dispersion or supplied noise, including unresolved function variation. */
    val responseVariance: Double,
) {
    /** Mixture approximation for function uncertainty; correlated trees are not independent observations. */
    val latentVariance: Double get() = disagreementVariance + leafMeanVariance

    /** Approximate response variance, including within-leaf variability. */
    val predictiveVariance: Double get() = latentVariance + responseVariance
}

/** A coherent sampled regression function, fixed over repeated and reordered queries. */
fun interface RegressionFunction {
    /** Evaluate one numeric feature vector. */
    fun predict(features: Vector): Double
}

/**
 * Immutable forest with leaf uncertainty estimated on original observation units.
 * Partition selection and bootstrap effects are not corrected; these components are approximate,
 * not a calibrated Bayesian posterior. Leaf sampling variance can overlap ensemble variation.
 * Response dispersion is kept separate from mean uncertainty.
 * Bootstrap multiplicity never increases the effective evidence in a leaf. More trees do not
 * divide sampling variance, since they reuse the same observations. Singleton unknown-noise leaves
 * contribute zero additional estimated variance, retaining ensemble disagreement as the fallback.
 *
 * @property forest Fitted immutable tree ensemble.
 */
class BatchForestModel internal constructor(
    val forest: ForestRegressionResult,
    private val estimates: List<LeafEstimates>,
) {
    /** Predict equal-weight component moments for one numeric query. */
    fun predictMoments(features: Vector): ForestMoments {
        val distribution = forest.predictDistribution(features)
        val leaves = estimates.map { it.find(features) }
        return ForestMoments(
            distribution.mean,
            distribution.variance,
            leaves.sumOf { it.varianceOfMean ?: 0.0 } / leaves.size,
            leaves.sumOf { maxOf(it.responseVariance, it.knownNoiseVariance ?: 0.0) } / leaves.size,
        )
    }

    /** Draw one tree and fixed Gaussian leaf-mean offsets; query order never changes the draw. */
    fun drawFunction(random: Random, includeLeafMeanVariance: Boolean = true): RegressionFunction {
        val index = random.nextInt(forest.trees.size)
        val sampled = estimates[index].draw(forest.trees[index].root, random, includeLeafMeanVariance)
        return RegressionFunction { sampled.predict(it) }
    }
}

/**
 * Fit a forest with separately estimated leaf-mean uncertainty and optional known observation noise.
 * Noise entries align with observations and may be NaN where unknown. Statistical weights express
 * importance; independent replicated responses must occupy separate rows. Empty input retains the
 * ordinary empty forest and zero component estimates. Additional fitting work is O(trees * n * depth).
 */
fun BatchRandomForest.fitModel(
    observations: List<RegressionObservation>,
    random: Random = Random.Default,
    noiseVariances: Vector? = null,
): BatchForestModel {
    require(noiseVariances == null || noiseVariances.size == observations.size)
    if (noiseVariances != null) {
        require(
            noiseVariances.toDoubleArray().all { it.isNaN() || (it >= 0.0 && it.isFinite()) },
        )
    }
    val forest = fit(observations, random)
    val rows = observations.indices.filter { observations[it].weight > 0.0 }
    fun estimate(node: TreeNodeResult, indices: List<Int>): LeafEstimates = when (node) {
        is TreeSplitResult -> {
            val (pos, neg) = indices.partition { node.split.direction(observations[it].features) }
            LeafEstimates.Branch(node.split, estimate(node.pos, pos), estimate(node.neg, neg))
        }

        is TreeLeafResult -> LeafEstimates.Leaf(
            if (indices.isEmpty()) {
                MeanEstimate(0.0, 0.0, 0.0, null, null)
            } else {
                estimateMean(
                    DenseVector.of(DoubleArray(indices.size) { observations[indices[it]].response }),
                    DenseVector.of(DoubleArray(indices.size) { observations[indices[it]].weight }),
                    noiseVariances?.let { noise -> DenseVector.of(DoubleArray(indices.size) { noise[indices[it]] }) },
                )
            },
        )
    }
    return BatchForestModel(forest, forest.trees.map { estimate(it.root, rows) })
}

internal sealed interface LeafEstimates {
    fun find(features: Vector): MeanEstimate
    fun draw(node: TreeNodeResult, random: Random, includeVariance: Boolean): RegressionFunction

    class Leaf(private val estimate: MeanEstimate) : LeafEstimates {
        override fun find(features: Vector): MeanEstimate = estimate
        override fun draw(node: TreeNodeResult, random: Random, includeVariance: Boolean): RegressionFunction {
            val mean = node.value.mean
            val value = if (includeVariance) random.nextNormal(mean, sqrt(estimate.varianceOfMean ?: 0.0)) else mean
            return RegressionFunction { value }
        }
    }
    class Branch(private val split: SerializableSplit, private val pos: LeafEstimates, private val neg: LeafEstimates) :
        LeafEstimates {
        override fun find(features: Vector): MeanEstimate = if (split.direction(
                features,
            )
        ) {
            pos.find(features)
        } else {
            neg.find(features)
        }
        override fun draw(node: TreeNodeResult, random: Random, includeVariance: Boolean): RegressionFunction {
            node as TreeSplitResult
            val left = pos.draw(node.pos, random, includeVariance)
            val right = neg.draw(node.neg, random, includeVariance)
            return RegressionFunction { if (split.direction(it)) left.predict(it) else right.predict(it) }
        }
    }
}
