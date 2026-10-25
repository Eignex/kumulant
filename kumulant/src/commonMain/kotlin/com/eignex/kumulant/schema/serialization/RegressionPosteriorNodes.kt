package com.eignex.kumulant.schema.serialization

import com.eignex.kumulant.stat.regression.RegressionPosterior
import com.eignex.kumulant.stat.regression.glm.*
import com.eignex.kumulant.stat.regression.tree.*

internal val regressionPosteriorNodes: List<WireNode> by lazy {
    listOf(
        WireNode(PointPosterior::class, "pointPosterior") { PointPosterior.serializer() },
        WireNode(FactorisedGaussian::class, "factorisedGaussian") { FactorisedGaussian.serializer() },
        WireNode(MultivariateGaussian::class, "multivariateGaussian") { MultivariateGaussian.serializer() },
        WireNode(LinUcb::class, "linUcb") { LinUcb.serializer() },
        WireNode(MeanTreePosterior::class, "meanTreePosterior") { MeanTreePosterior.serializer() },
        WireNode(ThompsonTreePosterior::class, "thompsonTreePosterior") { ThompsonTreePosterior.serializer() },
        WireNode(UcbTreePosterior::class, "ucbTreePosterior") { UcbTreePosterior.serializer() },
        WireNode(MeanForestPosterior::class, "meanForestPosterior") { MeanForestPosterior.serializer() },
        WireNode(ThompsonForestPosterior::class, "thompsonForestPosterior") { ThompsonForestPosterior.serializer() },
        WireNode(UcbForestPosterior::class, "ucbForestPosterior") { UcbForestPosterior.serializer() },
    )
}

internal object CanonicalRegressionPosteriorSerializer : CanonicalSerializer<RegressionPosterior<*>>(
    RegressionPosterior::class,
    { regressionPosteriorNodes },
)

internal object TreePosteriorSerializer : CanonicalSerializer<TreePosterior>(
    TreePosterior::class,
    {
        regressionPosteriorNodes.filter {
            it.type in setOf(
                MeanTreePosterior::class,
                ThompsonTreePosterior::class,
                UcbTreePosterior::class,
            )
        }
    },
)

internal object ForestPosteriorSerializer : CanonicalSerializer<ForestPosterior>(
    ForestPosterior::class,
    {
        regressionPosteriorNodes.filter {
            it.type in setOf(
                MeanForestPosterior::class,
                ThompsonForestPosterior::class,
                UcbForestPosterior::class,
            )
        }
    },
)
