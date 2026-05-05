package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stat.summary.Mean
import com.eignex.kumulant.stat.summary.Sum
import com.eignex.kumulant.stat.summary.Variance
import kotlin.test.Test
import kotlin.test.assertEquals

class StatSchemaConcurrencyTest {

    @Test
    fun `default schema leaves stats at None`() {
        val schema = object : StatSchema() {
            val sum by series(Sum())
            val mean by series(Mean())
        }
        for (spec in schema.specs) {
            assertEquals(Concurrency.None, spec.stat.concurrency)
        }
    }

    @Test
    fun `schema concurrency propagates to every registered stat`() {
        val schema = object : StatSchema(Concurrency.Strict) {
            val sum by series(Sum())
            val mean by series(Mean())
            val variance by series(Variance())
        }
        for (spec in schema.specs) {
            assertEquals(Concurrency.Strict, spec.stat.concurrency)
        }
    }

    @Test
    fun `schema concurrency overrides per-stat parameter`() {
        // The user passes Relaxed inline but the schema is Strict — schema wins.
        val schema = object : StatSchema(Concurrency.Strict) {
            val mean by series(Mean(Concurrency.Relaxed))
        }
        assertEquals(Concurrency.Strict, schema.specs.single().stat.concurrency)
    }

    @Test
    fun `nested schema keeps its own concurrency independent of parent`() {
        val inner = object : StatSchema(Concurrency.Strict) {
            val mean by series(Mean())
        }
        // Inner schema's specs already carry Strict.
        assertEquals(Concurrency.Strict, inner.specs.single().stat.concurrency)

        val parent = object : StatSchema(Concurrency.None) {
            val nested by group(inner)
        }
        // Parent schema itself stays None.
        assertEquals(Concurrency.None, parent.concurrency)
    }
}
