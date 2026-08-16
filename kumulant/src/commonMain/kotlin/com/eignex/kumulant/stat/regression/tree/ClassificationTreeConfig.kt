package com.eignex.kumulant.stat.regression.tree

import kotlinx.serialization.Serializable

/**
 * Classification analogue of [RegressionTreeConfig]. The same tunables, but the split [metric] defaults
 * to [GiniReduction] and the criterion is a [ClassificationSplitMetric].
 *
 * Every field except [metric] is described on [HoeffdingTreeConfig]. It used to be described here too,
 * in a copy that had already lost the explanations of what `tau` and `mtry` do.
 */
@Serializable
data class ClassificationTreeConfig(
    override val delta: Double = 0.05,
    override val deltaDecay: Double = 0.9,
    override val tau: Double = 0.05,
    override val minSamplesSplit: Double = 30.0,
    override val minSamplesLeaf: Double = 5.0,
    override val splitPeriod: Int = 10,
    override val maxDepth: Int = 16,
    override val maxNodes: Int = 1024,
    /** Split criterion; ranks candidates by the class impurity they remove. */
    val metric: ClassificationSplitMetric = GiniReduction,
    override val mtry: Int? = null,
) : HoeffdingTreeConfig
