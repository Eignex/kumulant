package com.eignex.kumulant.stat.regression.tree

import kotlinx.serialization.Serializable

/**
 * Classification analogue of [RegressionTreeConfig]. The same tunables, but the split [metric] defaults
 * to [GiniReduction] and the criterion is a [ClassificationSplitMetric].
 *
 * Every field except [metric] is described on [HoeffdingTreeConfig].
 */
@Serializable
data class ClassificationTreeConfig(
    override val delta: Double = HoeffdingDefaults.DELTA,
    override val deltaDecay: Double = HoeffdingDefaults.DELTA_DECAY,
    override val tau: Double = HoeffdingDefaults.TAU,
    override val minSamplesSplit: Double = HoeffdingDefaults.MIN_SAMPLES_SPLIT,
    override val minSamplesLeaf: Double = HoeffdingDefaults.MIN_SAMPLES_LEAF,
    override val splitPeriod: Int = HoeffdingDefaults.SPLIT_PERIOD,
    override val maxDepth: Int = HoeffdingDefaults.MAX_DEPTH,
    override val maxNodes: Int = HoeffdingDefaults.MAX_NODES,
    /** Split criterion; ranks candidates by the class impurity they remove. */
    val metric: ClassificationSplitMetric = GiniReduction,
    override val mtry: Int? = null,
) : HoeffdingTreeConfig
