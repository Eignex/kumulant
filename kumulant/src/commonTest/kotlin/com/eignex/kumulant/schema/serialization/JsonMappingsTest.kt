package com.eignex.kumulant.schema.serialization

import com.eignex.kumulant.schema.decay.Alpha
import com.eignex.kumulant.schema.decay.HalfLife
import com.eignex.kumulant.schema.spec.*
import com.eignex.skema.SchemaJson
import com.eignex.skema.serialization.toJsonSchema
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private val statSchema by lazy { StatSpecSerializer.toJsonSchema() }

private val exprSchema by lazy { ScalarExprSerializer.toJsonSchema() }

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
        assertEquals(JsonPrimitive("number"), properties.getValue("phi").jsonObject["type"])
        assertEquals(JsonPrimitive("string"), mode["type"])
        assertEquals(JsonArray(listOf(JsonPrimitive("Additive"), JsonPrimitive("Multiplicative"))), mode["enum"])
    }

    @Test
    fun `decay and smoothing encodings match their payload schemas`() {
        val specs = listOf<StatSpec>(
            DecayingSum(HalfLife(1_000)),
            EwmaVariance(Alpha(0.1)),
            Holt(Alpha(0.2)),
            Holt(Alpha(0.2), Alpha(0.3), phi = 0.9),
            SeasonalSmoothing(Alpha(0.1), Alpha(0.2), Alpha(0.3), period = 4),
        )

        for (spec in specs) {
            val (name, payload) = SchemaJson.encodeToJsonElement<StatSpec>(spec).jsonObject.entries.single()
            val schema = payloadSchema(name)
            val required = schema.getValue("required").jsonArray.map { it.jsonPrimitive.content }

            assertTrue(schema.getValue("properties").jsonObject.keys.containsAll(payload.jsonObject.keys), name)
            assertTrue(payload.jsonObject.keys.containsAll(required), name)
            assertEquals(spec, SchemaJson.decodeFromJsonElement<StatSpec>(JsonObject(mapOf(name to payload))))
        }
    }

    @Test
    fun `input and feedback schemas wrap their source tokens in operator keys`() {
        val cases = listOf(
            "input" to listOf("x", "y"),
            "feedback" to listOf("center", "scale", "low", "high", "index"),
        )
        val branches = alternatives(exprSchema)

        for ((name, tokens) in cases) {
            val operators = branches.filter { name in it["properties"]?.jsonObject.orEmpty() }

            assertEquals(
                tokens.map { token -> buildJsonObject { put("const", token) } },
                operators.map { it.getValue("properties").jsonObject.getValue(name) },
            )
            for (operator in operators) {
                assertEquals(JsonArray(listOf(JsonPrimitive(name))), operator["required"])
                assertEquals(JsonPrimitive(false), operator["additionalProperties"])
            }
        }
    }

    @Test
    fun `numeric literal schemas stay bare numbers`() {
        assertTrue(buildJsonObject { put("type", "number") } in alternatives(exprSchema))
    }

    @Test
    fun `only payloads accepting the empty object include name shorthands`() {
        val mapped = listOf(
            "decayingSum",
            "decayingMean",
            "decayingVariance",
            "ewma",
            "ewmaVariance",
            "holt",
            "seasonalSmoothing",
        )
        val shorthands = (alternatives(statSchema) + alternatives(exprSchema))
            .flatMap { it["anyOf"]?.jsonArray.orEmpty() }
            .mapNotNull { it.jsonObject["const"]?.jsonPrimitive?.content }

        assertTrue("mean" in shorthands)
        assertEquals("\"mean\"", SchemaJson.encodeToString<StatSpec>(Mean))
        assertTrue(shorthands.none { it in mapped + listOf("input", "feedback") }, shorthands.toString())
    }
}

private fun alternatives(document: JsonObject): List<JsonObject> {
    val root = document.getValue("\$ref").jsonPrimitive.content.removePrefix("#/\$defs/")
    val definition = document.getValue("\$defs").jsonObject.getValue(root).jsonObject
    return definition.getValue("anyOf").jsonArray.map { it.jsonObject }
}

private fun payloadSchema(name: String): JsonObject {
    val operator = alternatives(statSchema).single { name in it["properties"]?.jsonObject.orEmpty() }
    return operator.getValue("properties").jsonObject.getValue(name).jsonObject
}
