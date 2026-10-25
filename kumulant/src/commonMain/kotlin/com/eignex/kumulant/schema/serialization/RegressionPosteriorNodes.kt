package com.eignex.kumulant.schema.serialization

import com.eignex.kumulant.stat.regression.RegressionPosterior
import com.eignex.kumulant.stat.regression.glm.*
import com.eignex.kumulant.stat.regression.tree.*
import kotlinx.serialization.KSerializer

internal object LinearPosteriorSerializer : KSerializer<LinearPosterior<*>> by canonical(
    node<PointPosterior>("pointPosterior"),
    node<FactorisedGaussian>("factorisedGaussian"),
    node<MultivariateGaussian>("multivariateGaussian"),
    node<LinUcb>("linUcb"),
)

internal object TreePosteriorSerializer : KSerializer<TreePosterior> by canonical(
    node<MeanTreePosterior>("meanTreePosterior"),
    node<ThompsonTreePosterior>("thompsonTreePosterior"),
    node<UcbTreePosterior>("ucbTreePosterior"),
)

internal object ForestPosteriorSerializer : KSerializer<ForestPosterior> by canonical(
    node<MeanForestPosterior>("meanForestPosterior"),
    node<ThompsonForestPosterior>("thompsonForestPosterior"),
    node<UcbForestPosterior>("ucbForestPosterior"),
)

internal object CanonicalRegressionPosteriorSerializer : KSerializer<RegressionPosterior<*>> by canonical(
    { LinearPosteriorSerializer.nodes + TreePosteriorSerializer.nodes + ForestPosteriorSerializer.nodes },
)
