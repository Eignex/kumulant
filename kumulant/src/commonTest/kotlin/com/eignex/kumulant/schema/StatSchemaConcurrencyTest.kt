package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.schema.decay.*
import com.eignex.kumulant.schema.expr.*
import com.eignex.kumulant.schema.ops.*
import com.eignex.kumulant.schema.optimizer.*
import com.eignex.kumulant.schema.runtime.*
import com.eignex.kumulant.schema.spec.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Schemas are pure description; the deployment-knob [Concurrency] flows
 * through `config.materialize(schema.concurrency)` inside the
 * `*StatGroup(schema)` / `*ListStats(schema)` constructors. These tests
 * assert post-materialize concurrency propagates correctly.
 */
class StatSchemaConcurrencyTest {

    @Test
    fun `default schema materializes stats at None`() {
        val schema = object : StatSchema() {
            val sum by series(Sum)
            val mean by series(Mean)
        }
        for (spec in seriesSpecs(schema)) {
            assertEquals(Concurrency.None, spec.stat.concurrency)
        }
    }

    @Test
    fun `schema concurrency propagates to every materialized stat`() {
        val schema = object : StatSchema(Concurrency.Strict) {
            val sum by series(Sum)
            val mean by series(Mean)
            val variance by series(Variance)
        }
        for (spec in seriesSpecs(schema)) {
            assertEquals(Concurrency.Strict, spec.stat.concurrency)
        }
    }

    @Test
    fun `schema concurrency flows into the live StatGroup`() {
        val schema = object : StatSchema(Concurrency.Strict) {
            val sum by series(Sum)
        }
        val group = StatGroup(schema)
        group.update(1.0)
        group.update(2.0)
        assertEquals(3.0, group.read()[schema.sum].sum)
    }

    @Test
    fun `nested schema keeps its own concurrency independent of parent`() {
        val inner = object : StatSchema(Concurrency.Strict) {
            val mean by series(Mean)
        }
        val parent = object : StatSchema(Concurrency.None) {
            val nested by group(inner)
        }
        assertEquals(Concurrency.None, parent.concurrency)
        assertEquals(Concurrency.Strict, inner.concurrency)

        val innerSpecs = seriesSpecs(inner)
        assertTrue(innerSpecs.isNotEmpty())
        assertEquals(Concurrency.Strict, innerSpecs.single().stat.concurrency)
    }
}
