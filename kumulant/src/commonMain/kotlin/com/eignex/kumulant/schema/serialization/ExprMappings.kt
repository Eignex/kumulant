@file:OptIn(kotlinx.serialization.ExperimentalSerializationApi::class)

package com.eignex.kumulant.schema.serialization

import com.eignex.skema.serialization.JsonPayloadMapping
import kotlinx.serialization.json.*

internal class InputMapping(name: String) : JsonPayloadMapping {
    val token = JsonPrimitive(name)

    override fun encode(body: JsonObject): JsonElement = token

    override fun decode(payload: JsonElement): JsonObject {
        if (payload != token) invalid("unknown input or feedback source $payload")
        return JsonObject(emptyMap())
    }

    override fun payloadSchema(body: JsonObject): JsonObject = buildJsonObject { put("const", token) }
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

    override fun decode(payload: JsonElement): JsonObject = JsonObject(mapOf("v" to payload))

    override fun payloadSchema(body: JsonObject): JsonObject =
        body.getValue("properties").jsonObject.getValue("v").jsonObject
}
