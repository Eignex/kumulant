package com.eignex.kumulant.operation

import com.eignex.kumulant.stat.summary.VarianceStat
import kotlin.math.sqrt
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

private const val DELTA = 1e-9

class BandTest {

    @Test
    fun `band exposes center and scale from inner variance result`() {
        val s = VarianceStat().band(k = 2.0)
        listOf(0.0, 1.0, 2.0, 3.0, 4.0).forEach { s.update(it) }
        val r = s.read()
        assertEquals(2.0, r.center, DELTA)
        assertEquals(2.0, r.k, DELTA)
        // Variance of 0..4 with N=5 is 2.0; stdDev = sqrt(2).
        val stdDev = sqrt(2.0)
        assertEquals(stdDev, r.scale, DELTA)
        assertEquals(2.0 - 2.0 * stdDev, r.lower, DELTA)
        assertEquals(2.0 + 2.0 * stdDev, r.upper, DELTA)
    }

    @Test
    fun `band with k zero collapses to the center`() {
        val s = VarianceStat().band(k = 0.0)
        listOf(1.0, 2.0, 3.0).forEach { s.update(it) }
        val r = s.read()
        assertEquals(r.center, r.lower, DELTA)
        assertEquals(r.center, r.upper, DELTA)
    }

    @Test
    fun `band merge throws because the wrapper cannot round-trip BandResult`() {
        val s = VarianceStat().band(k = 1.0)
        s.update(1.0)
        assertFailsWith<IllegalStateException> {
            s.merge(BandResult(center = 0.0, scale = 0.0, k = 1.0, lower = 0.0, upper = 0.0))
        }
    }

    @Test
    fun `reset and create delegate to the inner`() {
        val tpl = VarianceStat().band(k = 1.0).apply {
            for (v in 1..10) update(v.toDouble())
        }
        tpl.reset()
        val r = tpl.read()
        assertEquals(0.0, r.center, DELTA)
        assertEquals(0.0, r.scale, DELTA)

        val fresh = tpl.create()
        fresh.update(5.0)
        assertEquals(5.0, fresh.read().center, DELTA)
        assertEquals(0.0, tpl.read().center, DELTA)
    }
}
