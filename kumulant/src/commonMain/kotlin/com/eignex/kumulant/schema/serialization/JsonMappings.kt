package com.eignex.kumulant.schema.serialization

import com.eignex.skema.serialization.JsonPayloadMapping
import kotlinx.serialization.json.*

internal class DecayMapping(private val field: String) : JsonPayloadMapping {
    override fun encode(body: JsonObject): JsonElement =
        JsonObject(mapOf(field to body.getValue("weighting").jsonObject.values.single()))

    override fun decode(payload: JsonElement): JsonObject {
        val body = payload.asObject()
        if (body.keys != setOf(field)) invalid("expected only '$field', got ${body.keys}")
        val parameter = if (field == "alpha") "alpha" else "durationMillis"
        return JsonObject(mapOf("weighting" to JsonObject(mapOf(parameter to body.getValue(field)))))
    }

    override fun payloadSchema(body: JsonObject): JsonObject {
        val parameter = buildJsonObject { put("type", if (field == "alpha") "number" else "integer") }
        return JsonObject(
            body + mapOf(
                "properties" to JsonObject(mapOf(field to parameter)),
                "required" to JsonArray(listOf(JsonPrimitive(field))),
            ),
        )
    }
}

internal object SmoothingMapping : JsonPayloadMapping {
    override fun encode(body: JsonObject): JsonElement = JsonObject(
        body.map { (key, value) ->
            val unwrapped = if (key.endsWith("Weighting")) value.jsonObject.getValue("alpha") else value
            key.removeSuffix("Weighting") to unwrapped
        }.toMap(),
    )

    override fun decode(payload: JsonElement): JsonObject = JsonObject(
        payload.asObject().map { (key, value) ->
            if (key.endsWith("Weighting")) invalid("unknown field '$key'")
            if (key in setOf("alpha", "beta", "gamma")) {
                "${key}Weighting" to JsonObject(mapOf("alpha" to value))
            } else {
                key to value
            }
        }.toMap(),
    )

    override fun payloadSchema(body: JsonObject): JsonObject {
        val properties = body.getValue("properties").jsonObject.map { (field, schema) ->
            val parameter = if (field.endsWith("Weighting")) {
                buildJsonObject { put("type", "number") }
            } else {
                schema
            }
            field.removeSuffix("Weighting") to parameter
        }.toMap()
        val required = body.getValue("required").jsonArray.map { field ->
            JsonPrimitive(field.jsonPrimitive.content.removeSuffix("Weighting"))
        }
        return JsonObject(body + mapOf("properties" to JsonObject(properties), "required" to JsonArray(required)))
    }
}
