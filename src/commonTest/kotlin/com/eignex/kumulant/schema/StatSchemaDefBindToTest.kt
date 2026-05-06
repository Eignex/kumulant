package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import kotlinx.serialization.encodeToString
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Demonstrates the "shared schema class on both sides" pattern: a
 * [StatSchema] subclass that both producer and consumer have as code lets
 * the producer ship pure-data JSON and the consumer recover full typing
 * via [StatSchemaDef.bindTo].
 */
private class SharedHttpMetrics : StatSchema() {
    val requests by series(SumConfig)
    val latencyMs by series(DDSketchConfig(probabilities = listOf(0.5, 0.99, 0.999)))
}

private class SharedHttpMetricsWithExtra : StatSchema() {
    val requests by series(SumConfig)
    val latencyMs by series(DDSketchConfig(probabilities = listOf(0.5, 0.99, 0.999)))
    val extra by series(MeanConfig)
}

class StatSchemaDefBindToTest {

    @Test
    fun `bindTo round-trips JSON into a typed schema with a live StatGroup`() {
        val producerJson = SchemaJson.encodeToString(SharedHttpMetrics().definition())

        val def = SchemaJson.decodeFromString<StatSchemaDef>(producerJson)
        val (schema, group) = def.bindTo(::SharedHttpMetrics, Concurrency.Strict)

        group.update(120.0)
        group.update(80.0)
        group.update(200.0)

        val snap = group.read()
        val sum = snap[schema.requests].sum
        val quantiles = snap[schema.latencyMs].quantiles

        assertEquals(400.0, sum)
        assertEquals(3, quantiles.size)
        assertTrue(quantiles.all { it > 0.0 })
    }

    @Test
    fun `bindTo fails loudly when the wire schema diverges from the local class`() {
        val wireJson = SchemaJson.encodeToString(SharedHttpMetrics().definition())
        val def = SchemaJson.decodeFromString<StatSchemaDef>(wireJson)

        val ex = assertFailsWith<IllegalArgumentException> {
            def.bindTo(::SharedHttpMetricsWithExtra, Concurrency.None)
        }
        assertTrue(ex.message!!.contains("SharedHttpMetricsWithExtra"))
        assertTrue(ex.message!!.contains("extra"))
    }

    // Concurrency is a runtime knob set by the consumer, not encoded on the wire.
    @Test
    fun `bindTo's concurrency arg, not the producer's, is what propagates`() {
        val def = SharedHttpMetrics().definition()
        val (_, group) = def.bindTo(::SharedHttpMetrics, Concurrency.Strict)
        assertEquals(Concurrency.Strict, group.concurrency)
    }
}
