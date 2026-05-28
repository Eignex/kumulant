package com.eignex.kumulant.stat.regression.tree

import kotlinx.serialization.Serializable

/**
 * Tunables for [RegressionTree] growth, shared by [DecisionTreeRegressionStat] and
 * [RandomForestRegressionStat].
 */
@Serializable
data class RegressionTreeConfig(
    /** Hoeffding-bound confidence threshold. Lower -> splits require more evidence. */
    val delta: Double = 0.05,
    /** Multiplicative decay applied to [delta] per depth; slows growth near leaves. */
    val deltaDecay: Double = 0.9,
    /** If the Hoeffding bound itself shrinks below this, the leaf may split even when
     *  the runner-up is close (the classic VFDT "tie-break" parameter). */
    val tau: Double = 0.05,
    /** Minimum total weighted samples at a leaf before split evaluation. */
    val minSamplesSplit: Double = 30.0,
    /** Minimum weighted samples on each side of a candidate split. */
    val minSamplesLeaf: Double = 5.0,
    /** Audit every Nth observation rather than every update. */
    val splitPeriod: Int = 10,
    /** Hard ceiling on tree depth. */
    val maxDepth: Int = 16,
    /** Hard ceiling on internal + leaf nodes. */
    val maxNodes: Int = 1024,
    /** Split criterion. */
    val metric: SplitMetric = VarianceReduction,
    /** Breiman-style random-subspace size: at every audit-leaf birth, draw a fresh random
     *  subset of this many candidates from the tree's full pool. `null` disables the trick. */
    val mtry: Int? = null,
)
