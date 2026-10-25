package com.eignex.kumulant.schema.serialization

import com.eignex.kumulant.stat.regression.RegressionPosterior
import com.eignex.kumulant.stat.regression.glm.*
import com.eignex.kumulant.stat.regression.tree.*

internal object LinearPosteriorSerializer : CanonicalSerializer<LinearPosterior<*>>(
    LinearPosterior::class,
    node<PointPosterior>("pointPosterior"),
    node<FactorisedGaussian>("factorisedGaussian"),
    node<MultivariateGaussian>("multivariateGaussian"),
    node<LinUcb>("linUcb"),
)

internal object TreePosteriorSerializer : CanonicalSerializer<TreePosterior>(
    TreePosterior::class,
    node<MeanTreePosterior>("meanTreePosterior"),
    node<ThompsonTreePosterior>("thompsonTreePosterior"),
    node<UcbTreePosterior>("ucbTreePosterior"),
)

internal object ForestPosteriorSerializer : CanonicalSerializer<ForestPosterior>(
    ForestPosterior::class,
    node<MeanForestPosterior>("meanForestPosterior"),
    node<ThompsonForestPosterior>("thompsonForestPosterior"),
    node<UcbForestPosterior>("ucbForestPosterior"),
)

internal object CanonicalRegressionPosteriorSerializer : CanonicalSerializer<RegressionPosterior<*>>(
    RegressionPosterior::class,
    { LinearPosteriorSerializer.nodes + TreePosteriorSerializer.nodes + ForestPosteriorSerializer.nodes },
)
