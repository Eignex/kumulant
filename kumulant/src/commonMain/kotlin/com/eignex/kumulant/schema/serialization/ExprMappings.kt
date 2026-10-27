@file:OptIn(kotlinx.serialization.ExperimentalSerializationApi::class)

package com.eignex.kumulant.schema.serialization

import com.eignex.skema.serialization.JsonNode
import com.eignex.skema.serialization.JsonPayloadMapping
import kotlinx.serialization.json.*

internal class InputMapping(name: String) : JsonPayloadMapping {
    val token = JsonPrimitive(name)

    override fun encode(body: JsonObject): JsonElement = token

    override fun decode(payload: JsonElement, path: String): JsonObject {
        if (payload != token) invalid(path, "unknown input or feedback source")
        return JsonObject(emptyMap())
    }

    override fun jsonSchema(node: JsonNode, body: JsonObject): JsonObject = buildJsonObject {
        put("type", "object")
        put("properties", buildJsonObject { put(node.name, buildJsonObject { put("const", token) }) })
        put("required", JsonArray(listOf(JsonPrimitive(node.name))))
        put("additionalProperties", false)
    }
}

internal object NumberMapping : JsonPayloadMapping {
    override fun encode(body: JsonObject): JsonElement {
        val literal = body.getValue("v").jsonPrimitive
        val value = literal.double
        if (!value.isFinite() || value % 1.0 != 0.0) return literal
        if (value.toBits() == (-0.0).toBits()) return JsonUnquotedLiteral("-0")
        if (value >= Long.MIN_VALUE.toDouble() && value < Long.MAX_VALUE.toDouble()) {
            return JsonPrimitive(value.toLong())
        }
        return JsonUnquotedLiteral(value.toString().replace(".0E", "E").removeSuffix(".0"))
    }

    override fun decode(payload: JsonElement, path: String): JsonObject = JsonObject(mapOf("v" to payload))

    override fun fieldPath(field: String, path: String): String = path

    override fun jsonSchema(node: JsonNode, body: JsonObject): JsonObject =
        body.getValue("properties").jsonObject.getValue("v").jsonObject
}
