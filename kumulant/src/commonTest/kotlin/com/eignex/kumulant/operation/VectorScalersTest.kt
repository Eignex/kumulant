package com.eignex.kumulant.operation

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.schema.ops.minMaxScaleFeatures
import com.eignex.kumulant.schema.ops.standardScaleFeatures
import com.eignex.kumulant.schema.ops.vectorized
import com.eignex.kumulant.schema.runtime.materialize
import com.eignex.kumulant.schema.spec.Sum
import com.eignex.kumulant.stat.summary.VarianceStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class VectorScalersTest {

    @Test
    fun `standardScaleFeatures matches hand-rolled per-coordinate z-scores`() {
        val dimensions = 3
        val scaled = Sum.vectorized(dimensions).standardScaleFeatures(dimensions).materialize()

        val updates = listOf(
            doubleArrayOf(1.0, 10.0, 100.0),
            doubleArrayOf(2.0, 20.0, 200.0),
            doubleArrayOf(3.0, 30.0, 300.0),
            doubleArrayOf(4.0, 40.0, 400.0),
        )
        for (u in updates) scaled.update(u)

        val refPrimaries = Array(dimensions) { VarianceStat() }
        val expectedSums = DoubleArray(dimensions)
        for (u in updates) {
            for (i in 0 until dimensions) {
                refPrimaries[i].update(u[i])
                val r = refPrimaries[i].read()
                expectedSums[i] += if (r.stdDev == 0.0) 0.0 else (u[i] - r.mean) / r.stdDev
            }
        }
        val actual = scaled.read().results
        for (i in 0 until dimensions) {
            assertEquals(expectedSums[i], actual[i].sum, DELTA, "coord $i")
        }
    }

    @Test
    fun `minMaxScaleFeatures maps each coordinate to the configured range`() {
        val dimensions = 2
        val scaled = Sum.vectorized(dimensions)
            .minMaxScaleFeatures(dimensions, targetLow = 0.0, targetHigh = 1.0)
            .materialize()
        scaled.update(doubleArrayOf(0.0, 100.0))
        scaled.update(doubleArrayOf(10.0, 200.0))
        scaled.update(doubleArrayOf(5.0, 150.0))
        val r = scaled.read().results
        assertEquals(1.5, r[0].sum, DELTA)
        assertEquals(1.5, r[1].sum, DELTA)
    }

    @Test
    fun `minMaxScaleFeatures rejects inverted range`() {
        assertFailsWith<IllegalArgumentException> {
            Sum.vectorized(2).minMaxScaleFeatures(2, targetLow = 1.0, targetHigh = 0.0)
        }
    }

    @Test
    fun `reset clears both primary fan-out and inner`() {
        val scaled = Sum.vectorized(2).standardScaleFeatures(2).materialize()
        scaled.update(doubleArrayOf(1.0, 10.0))
        scaled.update(doubleArrayOf(2.0, 20.0))
        scaled.reset()
        scaled.update(doubleArrayOf(5.0, 50.0))
        val r = scaled.read().results
        assertEquals(0.0, r[0].sum, DELTA)
        assertEquals(0.0, r[1].sum, DELTA)
    }

    @Test
    fun `create produces an independent stat`() {
        val tpl = Sum.vectorized(2).standardScaleFeatures(2).materialize()
        tpl.update(doubleArrayOf(1.0, 10.0))
        tpl.update(doubleArrayOf(3.0, 30.0))
        val fresh = tpl.create()
        fresh.update(doubleArrayOf(5.0, 50.0))
        val tplFirst = tpl.read().results[0].sum
        val freshFirst = fresh.read().results[0].sum
        assertTrue(tplFirst != freshFirst)
    }
}
