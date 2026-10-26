package com.eignex.kumulant.schema.serialization

import com.eignex.skema.serialization.CompactSerializer
import com.eignex.skema.serialization.JsonNode
import com.eignex.skema.serialization.JsonNodeCatalog
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.*
import kotlin.reflect.KClass

internal abstract class KumulantSerializer<T : Any>(base: KClass<T>, nodes: () -> List<JsonNode>) :
    CompactSerializer<T>(base.qualifiedName!!, base, catalog, nodes)

private val catalog = JsonNodeCatalog(::resolveStatNode) {
    listOf(
        SeriesStatSpecSerializer,
        PairedStatSpecSerializer,
        VectorStatSpecSerializer,
        DiscreteStatSpecSerializer,
        RegressionStatSpecSerializer,
        StatSpecSerializer,
        ScalarExprSerializer,
        BoolExprSerializer,
        VectorExprSerializer,
        DecayWeightingSpecSerializer,
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
}

private fun resolveStatNode(candidates: List<JsonNode>, payload: JsonElement, path: String): JsonNode {
    val innerPath = "$path.inner"
    val inner = payload.objectAt(path)["inner"] ?: invalid(innerPath, "missing inner spec")
    val innerNode = StatSpecSerializer.selectNode(inner, innerPath)
    return candidates.singleOrNull { statModalities[it.type] == statModalities[innerNode.type] }
        ?: invalid(innerPath, "operator '${candidates.first().name}' does not support this input modality")
}

internal fun JsonElement.objectAt(path: String): JsonObject = this as? JsonObject ?: invalid(path, "expected an object")

internal fun invalid(path: String, message: String): Nothing = throw SerializationException("$path: $message")
