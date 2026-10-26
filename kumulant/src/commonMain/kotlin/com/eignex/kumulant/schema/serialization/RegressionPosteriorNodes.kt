package com.eignex.kumulant.schema.serialization

import com.eignex.kumulant.stat.regression.RegressionPosterior
import com.eignex.kumulant.stat.regression.glm.*
import com.eignex.kumulant.stat.regression.tree.*
import com.eignex.skema.serialization.jsonNode

internal object LinearPosteriorSerializer : KumulantSerializer<LinearPosterior<*>>(LinearPosterior::class, {
    listOf(
        jsonNode<PointPosterior>("pointPosterior"),
        jsonNode<FactorisedGaussian>("factorisedGaussian"),
        jsonNode<MultivariateGaussian>("multivariateGaussian"),
        jsonNode<LinUcb>("linUcb"),
    )
})

internal object TreePosteriorSerializer : KumulantSerializer<TreePosterior>(TreePosterior::class, {
    listOf(
        jsonNode<MeanTreePosterior>("meanTreePosterior"),
        jsonNode<ThompsonTreePosterior>("thompsonTreePosterior"),
        jsonNode<UcbTreePosterior>("ucbTreePosterior"),
    )
})

internal object ForestPosteriorSerializer : KumulantSerializer<ForestPosterior>(ForestPosterior::class, {
    listOf(
        jsonNode<MeanForestPosterior>("meanForestPosterior"),
        jsonNode<ThompsonForestPosterior>("thompsonForestPosterior"),
        jsonNode<UcbForestPosterior>("ucbForestPosterior"),
    )
})

internal object CanonicalRegressionPosteriorSerializer : KumulantSerializer<RegressionPosterior<*>>(
    RegressionPosterior::class,
    { LinearPosteriorSerializer.nodes + TreePosteriorSerializer.nodes + ForestPosteriorSerializer.nodes },
)
