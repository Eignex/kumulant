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
    // Whole numbers print without a trailing ".0".
    override fun encode(body: JsonObject): JsonElement {
        val literal = body.getValue("v").jsonPrimitive
        val whole = literal.double.toLong()
        return if (whole.toDouble().toBits() == literal.double.toBits()) JsonPrimitive(whole) else literal
    }

    override fun decode(payload: JsonElement): JsonObject = JsonObject(mapOf("v" to payload))

    override fun payloadSchema(body: JsonObject): JsonObject =
        body.getValue("properties").jsonObject.getValue("v").jsonObject
}
