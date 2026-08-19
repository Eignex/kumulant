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

// Schemas are pure description. [Concurrency] is a deployment choice supplied to the
// `*StatGroup(schema, concurrency)` / `*ListStats(schema, concurrency)` constructors, and it applies to
// the whole tree the schema describes, nested groups included.
class StatSchemaConcurrencyTest {

    @Test
    fun `a schema materializes at None by default`() {
        val schema = object : StatSchema() {
            val sum by series(Sum)
            val mean by series(Mean)
        }
        for (spec in seriesSpecs(schema, Concurrency.None)) {
            assertEquals(Concurrency.None, spec.stat.concurrency)
        }
    }

    @Test
    fun `the requested concurrency reaches every materialized stat`() {
        val schema = object : StatSchema() {
            val sum by series(Sum)
            val mean by series(Mean)
            val variance by series(Variance)
        }
        for (spec in seriesSpecs(schema, Concurrency.Strict)) {
            assertEquals(Concurrency.Strict, spec.stat.concurrency)
        }
    }

    @Test
    fun `the requested concurrency flows into the live StatGroup`() {
        val schema = object : StatSchema() {
            val sum by series(Sum)
        }
        val group = StatGroup(schema, concurrency = Concurrency.Strict)
        group.update(1.0)
        group.update(2.0)
        assertEquals(3.0, group.read()[schema.sum].sum)
    }

    @Test
    fun `a nested schema materializes at the concurrency its parent was built with`() {
        val inner = object : StatSchema() {
            val mean by series(Mean)
        }
        val parent = object : StatSchema() {
            val nested by group(inner)
        }
        val nested = seriesSpecs(parent, Concurrency.Strict).single().stat
        assertEquals(Concurrency.Strict, nested.concurrency)
    }

    @Test
    fun `a group never reports a level its children were not built at`() {
        val schema = object : StatSchema() {
            val sum by series(Sum)
        }
        val group = StatGroup(schema, concurrency = Concurrency.Strict)
        assertEquals(Concurrency.Strict, group.concurrency)
        for (spec in seriesSpecs(schema, Concurrency.Strict)) {
            assertTrue(spec.stat.concurrency >= group.concurrency)
        }
    }

    @Test
    fun `one schema can back two groups at different levels`() {
        val schema = object : StatSchema() {
            val sum by series(Sum)
        }
        assertEquals(Concurrency.None, StatGroup(schema).concurrency)
        assertEquals(Concurrency.Strict, StatGroup(schema, Concurrency.Strict).concurrency)
    }
}
