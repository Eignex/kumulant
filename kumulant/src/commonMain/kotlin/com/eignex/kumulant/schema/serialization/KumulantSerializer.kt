package com.eignex.kumulant.schema.serialization

import com.eignex.kumulant.schema.KumulantJson
import com.eignex.skema.serialization.CompactSerializer
import com.eignex.skema.serialization.JsonNode
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.*
import kotlin.reflect.KClass

internal abstract class KumulantSerializer<T : Any>(base: KClass<T>, nodes: () -> List<JsonNode>) :
    CompactSerializer<T>(base.qualifiedName!!, base, KumulantJson.catalog, nodes)

internal fun resolveKumulantNode(candidates: List<JsonNode>, payload: JsonElement, path: String): JsonNode {
    val source = candidates.filter { it.mapping is InputMapping }
    if (source.isNotEmpty()) {
        return source.singleOrNull { (it.mapping as InputMapping).token == payload }
            ?: invalid(path, "unknown input or feedback source")
    }
    val innerPath = "$path.inner"
    val inner = payload.objectAt(path)["inner"] ?: invalid(innerPath, "missing inner spec")
    val innerNode = StatSpecSerializer.selectNode(inner, innerPath)
    return candidates.singleOrNull { statModalities[it.type] == statModalities[innerNode.type] }
        ?: invalid(innerPath, "operator '${candidates.first().name}' does not support this input modality")
}

internal fun JsonElement.objectAt(path: String): JsonObject = this as? JsonObject ?: invalid(path, "expected an object")

internal fun invalid(path: String, message: String): Nothing = throw SerializationException("$path: $message")
