package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.Vector
import com.eignex.kumulant.math.nextNormal
import com.eignex.kumulant.schema.serialization.ForestPosteriorSerializer
import com.eignex.kumulant.schema.serialization.TreePosteriorSerializer
import com.eignex.kumulant.stat.regression.RegressionPosterior
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.sqrt
import kotlin.random.Random

/**
 * RegressionTree-aware scorer: routes the query `x` to a leaf snapshot and turns its weighted-
 * variance summary into a single Double. Parallels the linear-side
 * [com.eignex.kumulant.stat.regression.glm.LinearPosterior] family for the tree regressor
 * shape.
 *
 * Concrete posteriors differ only in how they consume the leaf's `(mean, variance,
 * totalWeights)` triplet:
 *  - [MeanTreePosterior]: deterministic leaf mean.
 *  - [ThompsonTreePosterior]: Normal-Gamma draw built from the leaf's first two
 *    moments; the natural Thompson scheme when leaves accumulate normal-like rewards.
 *  - [UcbTreePosterior]: UCB-style mean + alpha * sqrt(variance / totalWeights) on
 *    the leaf's sampling-distribution-of-the-mean.
 */
@Serializable(with = TreePosteriorSerializer::class)
sealed interface TreePosterior : RegressionPosterior<TreeRegressionResult>

/** Score is the leaf's running mean; point estimate, no exploration. */
@Serializable
@SerialName("MeanTreePosterior")
data object MeanTreePosterior : TreePosterior {
    override fun evaluate(snapshot: TreeRegressionResult, x: Vector, rng: Random, exploration: Double): Double =
        snapshot.findLeaf(x).mean
}

// Blending prior variance preserves exploration for singleton leaves with zero empirical variance.
private fun blendedVariance(leaf: WeightedVarianceResult, priorWeight: Double, priorVariance: Double): Double {
    val n = leaf.totalWeights + priorWeight
    if (n <= 0.0) return priorVariance
    return (priorWeight * priorVariance + leaf.totalWeights * leaf.variance) / n
}

/**
 * Thompson sampling over the leaf's Normal-Gamma posterior. Given the leaf's pseudo-
 * count `n`, sample mean `m`, and sample variance `v`, draws are `mu ~ N(m, exploration *
 * v / max(n, 1))`; the posterior on the leaf mean assuming a Normal-Gamma conjugate. The variance is
 * blended with [priorVariance] on a [priorWeight] pseudo-count, so a leaf holding one observation - whose
 * sample variance is exactly zero - still has spread. `exploration = 0.0` collapses to the leaf mean.
 */
@Serializable
@SerialName("ThompsonTreePosterior")
data class ThompsonTreePosterior(
    /** Pseudo-count added to the leaf's totalWeights to avoid divide-by-zero on empty leaves. */
    val priorWeight: Double = 1.0,
    /** Prior variance applied when the leaf has effectively no signal. */
    val priorVariance: Double = 1.0,
) : TreePosterior {
    override fun evaluate(snapshot: TreeRegressionResult, x: Vector, rng: Random, exploration: Double): Double {
        val leaf = snapshot.findLeaf(x)
        if (exploration <= 0.0) return leaf.mean
        val n = leaf.totalWeights + priorWeight
        val sd = sqrt(exploration * blendedVariance(leaf, priorWeight, priorVariance) / n)
        return rng.nextNormal(leaf.mean, sd)
    }
}

/**
 * UCB-style score: `mean + exploration * sqrt(variance / (totalWeights + priorWeight))`.
 * The `sqrt(.)` term is the leaf's standard error of the mean; the prior-weight floor
 * keeps the bound finite at empty leaves.
 */
@Serializable
@SerialName("UcbTreePosterior")
data class UcbTreePosterior(
    /** Pseudo-count added to the leaf's totalWeights; floor for empty leaves. */
    val priorWeight: Double = 1.0,
    /** Prior variance used when the leaf has no signal yet. */
    val priorVariance: Double = 1.0,
) : TreePosterior {
    override fun evaluate(snapshot: TreeRegressionResult, x: Vector, rng: Random, exploration: Double): Double {
        val leaf = snapshot.findLeaf(x)
        val n = leaf.totalWeights + priorWeight
        return leaf.mean + exploration * sqrt(blendedVariance(leaf, priorWeight, priorVariance) / n)
    }
}

/** [TreePosterior] family ported to forests: every leaf snapshot the query routes to
 *  is merged into a single weighted-variance result, then scored with the tree-posterior
 *  semantics. Same options, applied to the ensembled leaf. */
@Serializable(with = ForestPosteriorSerializer::class)
sealed interface ForestPosterior : RegressionPosterior<ForestRegressionResult>

/** Forest counterpart to [MeanTreePosterior]. */
@Serializable
@SerialName("MeanForestPosterior")
data object MeanForestPosterior : ForestPosterior {
    override fun evaluate(snapshot: ForestRegressionResult, x: Vector, rng: Random, exploration: Double): Double =
        snapshot.findLeafMerged(x).mean
}

/** Forest counterpart to [ThompsonTreePosterior]. */
@Serializable
@SerialName("ThompsonForestPosterior")
data class ThompsonForestPosterior(
    /** Pseudo-count added to the leaf's totalWeights; floor for empty leaves. */
    val priorWeight: Double = 1.0,
    /** Prior variance used when the leaf has no signal yet. */
    val priorVariance: Double = 1.0,
) : ForestPosterior {
    override fun evaluate(snapshot: ForestRegressionResult, x: Vector, rng: Random, exploration: Double): Double {
        val leaf = snapshot.findLeafEvidence(x)
        if (exploration <= 0.0) return leaf.mean
        val n = leaf.totalWeights + priorWeight
        return rng.nextNormal(leaf.mean, sqrt(exploration * blendedVariance(leaf, priorWeight, priorVariance) / n))
    }
}

/** Forest counterpart to [UcbTreePosterior]. */
@Serializable
@SerialName("UcbForestPosterior")
data class UcbForestPosterior(
    /** Pseudo-count added to the leaf's totalWeights; floor for empty leaves. */
    val priorWeight: Double = 1.0,
    /** Prior variance used when the leaf has no signal yet. */
    val priorVariance: Double = 1.0,
) : ForestPosterior {
    override fun evaluate(snapshot: ForestRegressionResult, x: Vector, rng: Random, exploration: Double): Double {
        val leaf = snapshot.findLeafEvidence(x)
        val n = leaf.totalWeights + priorWeight
        return leaf.mean + exploration * sqrt(blendedVariance(leaf, priorWeight, priorVariance) / n)
    }
}
