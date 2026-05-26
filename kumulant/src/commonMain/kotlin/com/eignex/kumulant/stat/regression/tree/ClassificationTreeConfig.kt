package com.eignex.kumulant.stat.regression.tree

import kotlinx.serialization.Serializable

/**
 * Classification analogue of [RegressionTreeConfig]. Same tunables, but the split [metric]
 * defaults to [GiniReduction] and the criterion is a [ClassificationSplitMetric].
 */
@Serializable
data class ClassificationTreeConfig(
    /** Hoeffding-bound confidence threshold. */
    val delta: Double = 0.05,
    /** Multiplicative decay applied to [delta] per depth. */
    val deltaDecay: Double = 0.9,
    /** Hoeffding-bound shrinkage threshold for the VFDT tie-break. */
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
    val metric: ClassificationSplitMetric = GiniReduction,
    /** Breiman-style random-subspace size; `null` disables. */
    val mtry: Int? = null,
)
