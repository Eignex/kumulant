package com.eignex.kumulant.schema.serialization

import com.eignex.kumulant.schema.KumulantJson
import com.eignex.skema.serialization.CompactSerializer
import com.eignex.skema.serialization.JsonNode
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.*
import kotlin.reflect.KClass

internal abstract class KumulantSerializer<T : Any>(base: KClass<T>, nodes: () -> List<JsonNode>) :
    CompactSerializer<T>(base.qualifiedName!!, base, KumulantJson.catalog, nodes)

internal fun resolveKumulantNode(candidates: List<JsonNode>, payload: JsonElement): JsonNode {
    val source = candidates.filter { it.mapping is InputMapping }
    if (source.isNotEmpty()) {
        return source.singleOrNull { (it.mapping as InputMapping).token == payload }
            ?: invalid("unknown input or feedback source $payload")
    }
    val operator = candidates.first().name
    val inner = payload.asObject()["inner"] ?: invalid("'$operator' is missing its inner spec")
    val innerNode = StatSpecSerializer.selectNode(inner)
    return candidates.singleOrNull { statModalities[it.type] == statModalities[innerNode.type] }
        ?: invalid("'$operator' does not support the input modality of '${innerNode.name}'")
}

internal fun JsonElement.asObject(): JsonObject = this as? JsonObject ?: invalid("expected an object, got $this")

internal fun invalid(message: String): Nothing = throw SerializationException(message)
