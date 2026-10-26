package com.eignex.kumulant.schema.serialization

import com.eignex.skema.serialization.toJsonSchema
import kotlinx.serialization.json.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private val statSchema by lazy { StatSpecSerializer.toJsonSchema() }

class JsonMappingsTest {
    @Test
    fun `stat schema resolves every recursive definition`() {
        val definitions = statSchema.getValue("\$defs").jsonObject
        val references = mutableListOf<String>()
        fun collect(element: JsonElement) {
            when (element) {
                is JsonObject -> element.forEach { (name, value) ->
                    if (name == "\$ref") references += value.jsonPrimitive.content
                    collect(value)
                }
                is JsonArray -> element.forEach(::collect)
                else -> Unit
            }
        }

        collect(statSchema)

        assertTrue(references.isNotEmpty())
        for (reference in references) {
            assertTrue(reference.startsWith("#/\$defs/"), reference)
            assertTrue(reference.removePrefix("#/\$defs/") in definitions, reference)
        }
    }

    @Test
    fun `decay schemas require flattened numeric parameters`() {
        val cases = listOf(
            Triple("decayingSum", "halfLifeMillis", "integer"),
            Triple("decayingMean", "halfLifeMillis", "integer"),
            Triple("decayingVariance", "halfLifeMillis", "integer"),
            Triple("ewma", "alpha", "number"),
            Triple("ewmaVariance", "alpha", "number"),
        )

        for ((name, field, type) in cases) {
            val payload = payloadSchema(name)
            val properties = payload.getValue("properties").jsonObject

            assertEquals(setOf(field), properties.keys)
            assertEquals(type, properties.getValue(field).jsonObject.getValue("type").jsonPrimitive.content)
            assertEquals(JsonArray(listOf(JsonPrimitive(field))), payload["required"])
            assertEquals(JsonPrimitive(false), payload["additionalProperties"])
        }
    }

    @Test
    fun `half life schemas preserve the range of a long`() {
        val parameter = payloadSchema("decayingMean").getValue("properties").jsonObject
            .getValue("halfLifeMillis").jsonObject

        assertEquals(Long.MIN_VALUE, parameter.getValue("minimum").jsonPrimitive.long)
        assertEquals(Long.MAX_VALUE, parameter.getValue("maximum").jsonPrimitive.long)
    }

    @Test
    fun `smoothing schemas flatten required and optional weightings`() {
        val cases = listOf(
            "holt" to listOf("alpha"),
            "seasonalSmoothing" to listOf("alpha", "beta", "gamma", "period"),
        )

        for ((name, required) in cases) {
            val payload = payloadSchema(name)
            val properties = payload.getValue("properties").jsonObject

            assertEquals(JsonArray(required.map(::JsonPrimitive)), payload["required"])
            for (field in listOf("alpha", "beta", "gamma").filter { it in properties }) {
                assertEquals(JsonPrimitive("number"), properties.getValue(field).jsonObject["type"])
            }
            assertTrue(properties.keys.none { it.endsWith("Weighting") })
            assertEquals(JsonPrimitive(false), payload["additionalProperties"])
        }
    }

    @Test
    fun `smoothing schemas retain the ordinary parameter schemas`() {
        val properties = payloadSchema("seasonalSmoothing").getValue("properties").jsonObject
        val period = properties.getValue("period").jsonObject
        val mode = properties.getValue("mode").jsonObject

        assertEquals(JsonPrimitive("integer"), period["type"])
        assertEquals(Int.MIN_VALUE, period.getValue("minimum").jsonPrimitive.int)
        assertEquals(Int.MAX_VALUE, period.getValue("maximum").jsonPrimitive.int)
        assertEquals(JsonPrimitive("number"), properties.getValue("phi").jsonObject["type"])
        assertEquals(JsonPrimitive("string"), mode["type"])
        assertEquals(JsonArray(listOf(JsonPrimitive("Additive"), JsonPrimitive("Multiplicative"))), mode["enum"])
    }
}

private fun payloadSchema(name: String): JsonObject {
    val definitions = statSchema.getValue("\$defs").jsonObject
    val alternatives = definitions.getValue(StatSpecSerializer.serialName).jsonObject.getValue("anyOf").jsonArray
    val operator = alternatives.map { it.jsonObject }.single { name in it["properties"]?.jsonObject.orEmpty() }
    return operator.getValue("properties").jsonObject.getValue(name).jsonObject
}
