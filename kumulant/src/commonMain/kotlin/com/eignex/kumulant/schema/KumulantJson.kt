package com.eignex.kumulant.schema

import com.eignex.kumulant.schema.serialization.*
import com.eignex.skema.serialization.CompactSerializer
import com.eignex.skema.serialization.JsonNodeCatalog

/** Compact statistical and expression vocabularies for embedding in other serializable models. */
object KumulantJson {
    /** All vocabularies for composition with another library's compact serializers. */
    val serializers: List<CompactSerializer<*>>
        get() = listOf(
            SeriesStatSpecSerializer,
            PairedStatSpecSerializer,
            VectorStatSpecSerializer,
            DiscreteStatSpecSerializer,
            RegressionStatSpecSerializer,
            StatSpecSerializer,
            ScalarExprSerializer,
            BoolExprSerializer,
            VectorExprSerializer,
            OptimizerSpecSerializer,
            UnivariateBanditSpecSerializer,
            ContextualBanditSpecSerializer,
            SerializableSplitSerializer,
            BanditPolicySpecSerializer,
            ArmSerializer,
            PosteriorSerializer,
            LinkSerializer,
            PenaltySerializer,
            SplitMetricSerializer,
            ClassificationSplitMetricSerializer,
            LinearPosteriorSerializer,
            TreePosteriorSerializer,
            ForestPosteriorSerializer,
            CanonicalRegressionPosteriorSerializer,
        )

    /** Catalog for nested statistics, expressions, and JSON Schema generation. */
    val catalog: JsonNodeCatalog = JsonNodeCatalog(::resolveKumulantNode) { serializers }
}
