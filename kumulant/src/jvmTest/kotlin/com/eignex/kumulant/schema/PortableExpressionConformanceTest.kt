package com.eignex.kumulant.schema

import com.eignex.kumulant.schema.expr.*
import com.eignex.skema.SchemaJson
import kotlinx.serialization.json.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PortableExpressionConformanceTest {
    private val cases = Json.parseToJsonElement(
        requireNotNull(javaClass.getResourceAsStream("/portable-expression/conformance.json"))
            .bufferedReader().use { it.readText() },
    ).jsonObject.getValue("cases").jsonArray.map { it.jsonObject }

    @Test
    fun `binary64 environment evaluates accepted conformance fixtures`() {
        for (case in cases.filter { it["binary64"] != JsonPrimitive("reject") }) {
            val bindings = case.getValue("bindings").jsonObject.mapValues { (_, value) ->
                Const(value.jsonPrimitive.double)
            }
            val expression = case.getValue("expr")
            val expected = case.getValue("binary64").jsonPrimitive
            val name = case.getValue("name").jsonPrimitive.content

            if (case.getValue("type").jsonPrimitive.content == "boolean") {
                val predicate = SchemaJson.decodeFromJsonElement(BoolExpr.serializer(), expression).bind(bindings)
                assertEquals(expected.boolean, predicate.eval(0.0), name)
            } else {
                val scalar = SchemaJson.decodeFromJsonElement(ScalarExpr.serializer(), expression).bind(bindings)
                val actual = scalar.eval(0.0)
                if (expected.content == "NaN") {
                    assertTrue(actual.isNaN(), name)
                } else {
                    assertEquals(expected.content.toDouble(), actual, name)
                }
            }
        }
    }

    @Test
    fun `binary64 environment refuses rejected conformance fixtures`() {
        val accepted = cases.filter { it["binary64"] == JsonPrimitive("reject") }.filter { case ->
            runCatching {
                val bindings = case.getValue("bindings").jsonObject.mapValues { (_, value) ->
                    Const(value.jsonPrimitive.double)
                }
                val expression = case.getValue("expr")
                if (case.getValue("type").jsonPrimitive.content == "boolean") {
                    SchemaJson.decodeFromJsonElement(BoolExpr.serializer(), expression).bind(bindings).eval(0.0)
                } else {
                    SchemaJson.decodeFromJsonElement(ScalarExpr.serializer(), expression).bind(bindings).eval(0.0)
                }
            }.isSuccess
        }

        assertEquals(emptyList(), accepted.map { it.getValue("name").jsonPrimitive.content })
    }

    @Test
    fun `accepted portable fixtures encode to their canonical shapes`() {
        for (case in cases.filter { it["binary64"] != JsonPrimitive("reject") }) {
            val expression = case.getValue("expr")
            val encoded = if (case.getValue("type").jsonPrimitive.content == "boolean") {
                SchemaJson.encodeToJsonElement(
                    BoolExpr.serializer(),
                    SchemaJson.decodeFromJsonElement(BoolExpr.serializer(), expression),
                )
            } else {
                SchemaJson.encodeToJsonElement(
                    ScalarExpr.serializer(),
                    SchemaJson.decodeFromJsonElement(ScalarExpr.serializer(), expression),
                )
            }

            assertEquals(case["canonical"] ?: expression, encoded, case.getValue("name").jsonPrimitive.content)
        }
    }
}
