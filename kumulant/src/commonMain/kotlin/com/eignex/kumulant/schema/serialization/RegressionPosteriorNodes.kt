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
    )
})

internal object TreePosteriorSerializer : KumulantSerializer<TreePosterior>(TreePosterior::class, {
    listOf(
        jsonNode<MeanTreePosterior>("meanTreePosterior"),
        jsonNode<ThompsonTreePosterior>("thompsonTreePosterior"),
        jsonNode<UcbTreePosterior>("ucbTreePosterior"),
    )
})

internal object CanonicalRegressionPosteriorSerializer : KumulantSerializer<RegressionPosterior<*>>(
    RegressionPosterior::class,
    { LinearPosteriorSerializer.nodes + listOf(jsonNode<LinUcb>("linUcb")) + TreePosteriorSerializer.nodes },
)
