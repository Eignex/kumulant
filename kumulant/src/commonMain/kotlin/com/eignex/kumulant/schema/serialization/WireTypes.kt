package com.eignex.kumulant.schema.serialization

internal fun wireNodes(serialName: String): List<WireNode>? = when (serialName) {
    "com.eignex.kumulant.schema.spec.StatSpec" -> statSpecNodes
    "com.eignex.kumulant.schema.expr.ScalarExpr" -> scalarExprNodes
    "com.eignex.kumulant.schema.expr.BoolExpr" -> boolExprNodes
    "com.eignex.kumulant.schema.expr.VectorExpr" -> vectorExprNodes
    "com.eignex.kumulant.schema.decay.DecayWeightingSpec" -> decayWeightingSpecNodes
    "com.eignex.kumulant.schema.optimizer.OptimizerSpec" -> optimizerSpecNodes
    "com.eignex.kumulant.bandit.univariate.UnivariateBanditSpec" -> univariateBanditSpecNodes
    "com.eignex.kumulant.bandit.contextual.ContextualBanditSpec" -> contextualBanditSpecNodes
    "com.eignex.kumulant.stat.regression.tree.SerializableSplit" -> serializableSplitNodes
    "com.eignex.kumulant.bandit.univariate.BanditPolicySpec" -> banditPolicySpecNodes
    "com.eignex.kumulant.bandit.univariate.Arm" -> armNodes
    "com.eignex.kumulant.bandit.univariate.Posterior" -> posteriorNodes
    "com.eignex.kumulant.stat.regression.glm.LinearPosterior" -> linearPosteriorNodes
    "com.eignex.kumulant.stat.regression.glm.Link" -> linkNodes
    "com.eignex.kumulant.stat.regression.glm.Penalty" -> penaltyNodes
    "com.eignex.kumulant.schema.spec.SeriesStatSpec" -> seriesStatSpecNodes
    "com.eignex.kumulant.schema.spec.PairedStatSpec" -> pairedStatSpecNodes
    "com.eignex.kumulant.schema.spec.VectorStatSpec" -> vectorStatSpecNodes
    "com.eignex.kumulant.schema.spec.DiscreteStatSpec" -> discreteStatSpecNodes
    "com.eignex.kumulant.schema.spec.RegressionStatSpec" -> regressionStatSpecNodes
    "com.eignex.kumulant.stat.regression.tree.SplitMetric" -> SplitMetricSerializer.nodes
    "com.eignex.kumulant.stat.regression.tree.ClassificationSplitMetric" -> ClassificationSplitMetricSerializer.nodes
    "com.eignex.kumulant.stat.regression.tree.TreePosterior" -> TreePosteriorSerializer.nodes
    "com.eignex.kumulant.stat.regression.tree.ForestPosterior" -> ForestPosteriorSerializer.nodes
    "com.eignex.kumulant.stat.regression.RegressionPosterior" -> regressionPosteriorNodes
    else -> null
}
