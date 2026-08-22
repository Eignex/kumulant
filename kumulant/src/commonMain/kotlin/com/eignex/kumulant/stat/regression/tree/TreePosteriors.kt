package com.eignex.kumulant.stat.regression.tree

import com.eignex.koblas.core.F64VectorView
import com.eignex.kumulant.math.nextNormal
import com.eignex.kumulant.stat.regression.RegressionPosterior
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
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
sealed interface TreePosterior : RegressionPosterior<TreeRegressionResult>

/** Score is the leaf's running mean; point estimate, no exploration. */
data object MeanTreePosterior : TreePosterior {
    override fun evaluate(snapshot: TreeRegressionResult, x: F64VectorView, rng: Random, exploration: Double): Double =
        snapshot.findLeaf(x).mean
}

/**
 * Merged-leaf weight deflated by the number of trees that contributed it.
 *
 * [ForestRegressionResult.findLeafMerged] folds the leaf every tree routes `x` to into one accumulator,
 * but under bagging those are N correlated views of a single sample rather than N samples, so the merged
 * weight overstates the evidence by roughly the tree count. Dividing it back out keeps the posterior's
 * standard error at the honest `sqrt(variance / observations)` instead of shrinking it by
 * `sqrt(nbrTrees)` and under-exploring by the same factor.
 */
private fun effectiveWeight(leaf: WeightedVarianceResult, snapshot: ForestRegressionResult): Double =
    leaf.totalWeights / snapshot.trees.size

/**
 * Leaf variance blended with [priorVariance] on a [priorWeight] pseudo-count.
 *
 * Swapping in the prior only for a completely empty leaf leaves a hole one observation wide: a leaf
 * holding exactly one sample reports `totalWeights = 1` with a variance of exactly `0`, so a Thompson
 * draw collapses to a point and a UCB bound loses its bonus - on the leaves that need exploration most.
 * Driving a contextual bandit, an arm whose single observation was unlucky is then never picked again,
 * so the leaf never gets a second observation and the state is absorbing. Blending also removes the
 * discontinuity at `totalWeights == 0`, where the two branches disagreed.
 */
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
data class ThompsonTreePosterior(
    /** Pseudo-count added to the leaf's totalWeights to avoid divide-by-zero on empty leaves. */
    val priorWeight: Double = 1.0,
    /** Prior variance applied when the leaf has effectively no signal. */
    val priorVariance: Double = 1.0,
) : TreePosterior {
    override fun evaluate(snapshot: TreeRegressionResult, x: F64VectorView, rng: Random, exploration: Double): Double {
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
data class UcbTreePosterior(
    /** Pseudo-count added to the leaf's totalWeights; floor for empty leaves. */
    val priorWeight: Double = 1.0,
    /** Prior variance used when the leaf has no signal yet. */
    val priorVariance: Double = 1.0,
) : TreePosterior {
    override fun evaluate(snapshot: TreeRegressionResult, x: F64VectorView, rng: Random, exploration: Double): Double {
        val leaf = snapshot.findLeaf(x)
        val n = leaf.totalWeights + priorWeight
        return leaf.mean + exploration * sqrt(blendedVariance(leaf, priorWeight, priorVariance) / n)
    }
}

/** [TreePosterior] family ported to forests: every leaf snapshot the query routes to
 *  is merged into a single weighted-variance result, then scored with the tree-posterior
 *  semantics. Same options, applied to the ensembled leaf. */
sealed interface ForestPosterior : RegressionPosterior<ForestRegressionResult>

/** Forest counterpart to [MeanTreePosterior]. */
data object MeanForestPosterior : ForestPosterior {
    override fun evaluate(
        snapshot: ForestRegressionResult,
        x: F64VectorView,
        rng: Random,
        exploration: Double,
    ): Double = snapshot.findLeafMerged(x).mean
}

/** Forest counterpart to [ThompsonTreePosterior]. */
data class ThompsonForestPosterior(
    /** Pseudo-count added to the leaf's totalWeights; floor for empty leaves. */
    val priorWeight: Double = 1.0,
    /** Prior variance used when the leaf has no signal yet. */
    val priorVariance: Double = 1.0,
) : ForestPosterior {
    override fun evaluate(
        snapshot: ForestRegressionResult,
        x: F64VectorView,
        rng: Random,
        exploration: Double,
    ): Double {
        val leaf: WeightedVarianceResult = snapshot.findLeafMerged(x)
        if (exploration <= 0.0) return leaf.mean
        val n = effectiveWeight(leaf, snapshot) + priorWeight
        return rng.nextNormal(leaf.mean, sqrt(exploration * blendedVariance(leaf, priorWeight, priorVariance) / n))
    }
}

/** Forest counterpart to [UcbTreePosterior]. */
data class UcbForestPosterior(
    /** Pseudo-count added to the leaf's totalWeights; floor for empty leaves. */
    val priorWeight: Double = 1.0,
    /** Prior variance used when the leaf has no signal yet. */
    val priorVariance: Double = 1.0,
) : ForestPosterior {
    override fun evaluate(
        snapshot: ForestRegressionResult,
        x: F64VectorView,
        rng: Random,
        exploration: Double,
    ): Double {
        val leaf = snapshot.findLeafMerged(x)
        val n = effectiveWeight(leaf, snapshot) + priorWeight
        return leaf.mean + exploration * sqrt(blendedVariance(leaf, priorWeight, priorVariance) / n)
    }
}
