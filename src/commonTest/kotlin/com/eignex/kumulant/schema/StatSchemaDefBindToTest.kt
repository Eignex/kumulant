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
        // Producer: serialize the schema definition.
        val producerJson = SchemaJson.encodeToString(SharedHttpMetrics().definition())

        // Consumer: deserialize, bind back to the shared SharedHttpMetrics class.
        val def = SchemaJson.decodeFromString<StatSchemaDef>(producerJson)
        val (schema, group) = def.bindTo(::SharedHttpMetrics, Concurrency.Strict)

        // Drive the live group.
        group.update(120.0)
        group.update(80.0)
        group.update(200.0)

        // Read back through *typed* keys taken from the local SharedHttpMetrics instance.
        val snap = group.read()
        val sum = snap[schema.requests].sum
        val quantiles = snap[schema.latencyMs].quantiles

        assertEquals(400.0, sum)
        assertEquals(3, quantiles.size)
        assertTrue(quantiles.all { it > 0.0 })
    }

    @Test
    fun `bindTo fails loudly when the wire schema diverges from the local class`() {
        // Wire is SharedHttpMetrics; consumer tries to bind to SharedHttpMetricsWithExtra (extra entry).
        val wireJson = SchemaJson.encodeToString(SharedHttpMetrics().definition())
        val def = SchemaJson.decodeFromString<StatSchemaDef>(wireJson)

        val ex = assertFailsWith<IllegalArgumentException> {
            def.bindTo(::SharedHttpMetricsWithExtra, Concurrency.None)
        }
        assertTrue(ex.message!!.contains("SharedHttpMetricsWithExtra"))
        assertTrue(ex.message!!.contains("extra"))
    }

    @Test
    fun `bindTo works with strict concurrency propagating to materialized stats`() {
        val def = SharedHttpMetrics().definition()
        val (_, group) = def.bindTo(::SharedHttpMetrics, Concurrency.Strict)
        // Concurrency on the wire-materialized group propagates from the bindTo argument,
        // not from the producer's local schema (which was Concurrency.None).
        assertEquals(Concurrency.Strict, group.concurrency)
    }
}
