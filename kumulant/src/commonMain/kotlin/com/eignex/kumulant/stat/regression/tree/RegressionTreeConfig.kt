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
    override val delta: Double = 0.05,
    override val deltaDecay: Double = 0.9,
    override val tau: Double = 0.05,
    override val minSamplesSplit: Double = 30.0,
    override val minSamplesLeaf: Double = 5.0,
    override val splitPeriod: Int = 10,
    override val maxDepth: Int = 16,
    override val maxNodes: Int = 1024,
    /** Split criterion; ranks candidates by the variance they remove. */
    val metric: SplitMetric = VarianceReduction,
    override val mtry: Int? = null,
) : HoeffdingTreeConfig
