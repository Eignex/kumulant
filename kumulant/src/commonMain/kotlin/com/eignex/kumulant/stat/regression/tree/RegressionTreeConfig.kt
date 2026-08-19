package com.eignex.kumulant.stat.regression.tree

import kotlinx.serialization.Serializable

/**
 * Tunables for [RegressionTree] growth, shared by [DecisionTreeRegressionStat] and
 * [RandomForestRegressionStat].
 *
 * Every field except [metric] is described on [HoeffdingTreeConfig], which the classification
 * counterpart also implements.
 */
@Serializable
data class RegressionTreeConfig(
    override val delta: Double = HoeffdingDefaults.DELTA,
    override val deltaDecay: Double = HoeffdingDefaults.DELTA_DECAY,
    override val tau: Double = HoeffdingDefaults.TAU,
    override val minSamplesSplit: Double = HoeffdingDefaults.MIN_SAMPLES_SPLIT,
    override val minSamplesLeaf: Double = HoeffdingDefaults.MIN_SAMPLES_LEAF,
    override val splitPeriod: Int = HoeffdingDefaults.SPLIT_PERIOD,
    override val maxDepth: Int = HoeffdingDefaults.MAX_DEPTH,
    override val maxNodes: Int = HoeffdingDefaults.MAX_NODES,
    /** Split criterion; ranks candidates by the variance they remove. */
    val metric: SplitMetric = VarianceReduction,
    override val mtry: Int? = null,
) : HoeffdingTreeConfig {
    init {
        requireValidBoundParameters()
    }
}
