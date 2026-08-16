package com.eignex.kumulant.operation

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.stat.summary.SumStat
import com.eignex.kumulant.stat.summary.VarianceStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class VectorScalersTest {

    @Test
    fun `standardScaleFeatures matches hand-rolled per-coordinate z-scores`() {
        val dimensions = 3
        val tpl = VectorizedStat(dimensions, SumStat())
        val scaled = tpl.standardScaleFeatures(dimensions)

        val updates = listOf(
            doubleArrayOf(1.0, 10.0, 100.0),
            doubleArrayOf(2.0, 20.0, 200.0),
            doubleArrayOf(3.0, 30.0, 300.0),
            doubleArrayOf(4.0, 40.0, 400.0),
        )
        for (u in updates) scaled.update(u)

        // Reproduce: per-coordinate VarianceStat sees the same updates; the z-scored value
        // forwarded to the inner sum at each step is `(value - mean) / stdDev` (or 0 while
        // stdDev is still 0).
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
        val tpl = VectorizedStat(dimensions, SumStat())
        val scaled = tpl.minMaxScaleFeatures(dimensions, targetLow = 0.0, targetHigh = 1.0)
        scaled.update(doubleArrayOf(0.0, 100.0))
        scaled.update(doubleArrayOf(10.0, 200.0))
        scaled.update(doubleArrayOf(5.0, 150.0))
        // Per coordinate, after the second update min/max are observed; third update maps to 0.5.
        // Coord 0: 0 -> targetLow=0 (warm-up), 10 -> 1.0, 5 -> 0.5. Sum = 1.5.
        // Coord 1: 100 -> 0 (warm-up), 200 -> 1.0, 150 -> 0.5. Sum = 1.5.
        val r = scaled.read().results
        assertEquals(1.5, r[0].sum, DELTA)
        assertEquals(1.5, r[1].sum, DELTA)
    }

    @Test
    fun `minMaxScaleFeatures rejects inverted range`() {
        val tpl = VectorizedStat(2, SumStat())
        assertFailsWith<IllegalArgumentException> {
            tpl.minMaxScaleFeatures(2, targetLow = 1.0, targetHigh = 0.0)
        }
    }

    @Test
    fun `reset clears both primary fan-out and inner`() {
        val tpl = VectorizedStat(2, SumStat())
        val scaled = tpl.standardScaleFeatures(2)
        scaled.update(doubleArrayOf(1.0, 10.0))
        scaled.update(doubleArrayOf(2.0, 20.0))
        scaled.reset()
        scaled.update(doubleArrayOf(5.0, 50.0))
        // First post-reset update is the warm-up zero per coordinate.
        val r = scaled.read().results
        assertEquals(0.0, r[0].sum, DELTA)
        assertEquals(0.0, r[1].sum, DELTA)
    }

    @Test
    fun `create produces an independent stat`() {
        val tpl = VectorizedStat(2, SumStat()).standardScaleFeatures(2)
        tpl.update(doubleArrayOf(1.0, 10.0))
        tpl.update(doubleArrayOf(3.0, 30.0))
        val fresh = tpl.create()
        fresh.update(doubleArrayOf(5.0, 50.0))
        val tplFirst = tpl.read().results[0].sum
        val freshFirst = fresh.read().results[0].sum
        assertTrue(tplFirst != freshFirst)
    }
}
