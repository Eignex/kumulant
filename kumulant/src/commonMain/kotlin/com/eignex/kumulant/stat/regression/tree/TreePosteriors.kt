package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.math.VectorView
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
 *    moments — the natural Thompson scheme when leaves accumulate normal-like rewards.
 *  - [UcbTreePosterior]: UCB-style mean + alpha * sqrt(variance / totalWeights) on
 *    the leaf's sampling-distribution-of-the-mean.
 */
sealed interface TreePosterior : RegressionPosterior<TreeRegressionResult>

/** Score is the leaf's running mean — point estimate, no exploration. */
data object MeanTreePosterior : TreePosterior {
    override fun evaluate(snapshot: TreeRegressionResult, x: VectorView, rng: Random, exploration: Double): Double =
        snapshot.findLeaf(x).mean
}

/**
 * Thompson sampling over the leaf's Normal-Gamma posterior. Given the leaf's pseudo-
 * count `n`, sample mean `m`, and sample variance `v`, draws are `mu ~ N(m, exploration *
 * v / max(n, 1))` — the posterior on the leaf mean assuming a Normal-Gamma conjugate
 * with weak prior. `exploration = 0.0` collapses to the leaf mean.
 */
data class ThompsonTreePosterior(
    /** Pseudo-count added to the leaf's totalWeights to avoid divide-by-zero on empty leaves. */
    val priorWeight: Double = 1.0,
    /** Prior variance applied when the leaf has effectively no signal. */
    val priorVariance: Double = 1.0,
) : TreePosterior {
    override fun evaluate(snapshot: TreeRegressionResult, x: VectorView, rng: Random, exploration: Double): Double {
        val leaf = snapshot.findLeaf(x)
        if (exploration <= 0.0) return leaf.mean
        val n = leaf.totalWeights + priorWeight
        val v = if (leaf.totalWeights > 0.0) leaf.variance else priorVariance
        val sd = sqrt(exploration * v / n)
        return rng.nextNormal(leaf.mean, sd)
    }
}

/**
 * UCB-style score: `mean + exploration * sqrt(variance / (totalWeights + priorWeight))`.
 * The `sqrt(.)` term is the leaf's standard error of the mean; the prior-weight floor
 * keeps the bound finite at empty leaves.
 */
data class UcbTreePosterior(
    /** Pseudo-count added to the leaf's totalWeights — floor for empty leaves. */
    val priorWeight: Double = 1.0,
    /** Prior variance used when the leaf has no signal yet. */
    val priorVariance: Double = 1.0,
) : TreePosterior {
    override fun evaluate(snapshot: TreeRegressionResult, x: VectorView, rng: Random, exploration: Double): Double {
        val leaf = snapshot.findLeaf(x)
        val n = leaf.totalWeights + priorWeight
        val v = if (leaf.totalWeights > 0.0) leaf.variance else priorVariance
        return leaf.mean + exploration * sqrt(v / n)
    }
}

/** [TreePosterior] family ported to forests: every leaf snapshot the query routes to
 *  is merged into a single weighted-variance result, then scored with the tree-posterior
 *  semantics. Same options, applied to the ensembled leaf. */
sealed interface ForestPosterior : RegressionPosterior<ForestRegressionResult>

/** Forest counterpart to [MeanTreePosterior]. */
data object MeanForestPosterior : ForestPosterior {
    override fun evaluate(snapshot: ForestRegressionResult, x: VectorView, rng: Random, exploration: Double): Double =
        snapshot.findLeafMerged(x).mean
}

/** Forest counterpart to [ThompsonTreePosterior]. */
data class ThompsonForestPosterior(
    /** Pseudo-count added to the leaf's totalWeights — floor for empty leaves. */
    val priorWeight: Double = 1.0,
    /** Prior variance used when the leaf has no signal yet. */
    val priorVariance: Double = 1.0,
) : ForestPosterior {
    override fun evaluate(snapshot: ForestRegressionResult, x: VectorView, rng: Random, exploration: Double): Double {
        val leaf: WeightedVarianceResult = snapshot.findLeafMerged(x)
        if (exploration <= 0.0) return leaf.mean
        val n = leaf.totalWeights + priorWeight
        val v = if (leaf.totalWeights > 0.0) leaf.variance else priorVariance
        return rng.nextNormal(leaf.mean, sqrt(exploration * v / n))
    }
}

/** Forest counterpart to [UcbTreePosterior]. */
data class UcbForestPosterior(
    /** Pseudo-count added to the leaf's totalWeights — floor for empty leaves. */
    val priorWeight: Double = 1.0,
    /** Prior variance used when the leaf has no signal yet. */
    val priorVariance: Double = 1.0,
) : ForestPosterior {
    override fun evaluate(snapshot: ForestRegressionResult, x: VectorView, rng: Random, exploration: Double): Double {
        val leaf = snapshot.findLeafMerged(x)
        val n = leaf.totalWeights + priorWeight
        val v = if (leaf.totalWeights > 0.0) leaf.variance else priorVariance
        return leaf.mean + exploration * sqrt(v / n)
    }
}
