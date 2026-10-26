package com.eignex.kumulant.schema.serialization

import com.eignex.skema.serialization.JsonPayloadMapping
import kotlinx.serialization.json.*

internal class DecayMapping(private val field: String) : JsonPayloadMapping {
    override fun encode(body: JsonObject): JsonElement =
        JsonObject(mapOf(field to body.getValue("weighting").jsonObject.values.single()))

    override fun decode(payload: JsonElement, path: String): JsonObject {
        val body = payload.objectAt(path)
        if (body.keys != setOf(field)) invalid(path, "expected only '$field'")
        val parameter = if (field == "alpha") "alpha" else "durationMillis"
        return JsonObject(mapOf("weighting" to JsonObject(mapOf(parameter to body.getValue(field)))))
    }

    override fun fieldPath(field: String, path: String): String = path
}

internal object SmoothingMapping : JsonPayloadMapping {
    override fun encode(body: JsonObject): JsonElement = JsonObject(
        body.map { (key, value) ->
            val unwrapped = if (key.endsWith("Weighting")) value.jsonObject.getValue("alpha") else value
            key.removeSuffix("Weighting") to unwrapped
        }.toMap(),
    )

    override fun decode(payload: JsonElement, path: String): JsonObject = JsonObject(
        payload.objectAt(path).map { (key, value) ->
            if (key.endsWith("Weighting")) invalid("$path.$key", "unknown field '$key'")
            if (key in setOf("alpha", "beta", "gamma")) {
                "${key}Weighting" to JsonObject(mapOf("alpha" to value))
            } else {
                key to value
            }
        }.toMap(),
    )

    override fun fieldPath(field: String, path: String): String = "$path.${field.removeSuffix("Weighting")}"
}
