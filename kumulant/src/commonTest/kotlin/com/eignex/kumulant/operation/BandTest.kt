package com.eignex.kumulant.operation

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.schema.BandResult
import com.eignex.kumulant.stat.summary.VarianceStat
import kotlin.math.sqrt
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

class BandTest {

    @Test
    fun `band exposes center and scale from inner variance result`() {
        val s = VarianceStat().band(k = 2.0)
        listOf(0.0, 1.0, 2.0, 3.0, 4.0).forEach { s.update(it) }
        val r = s.read()
        assertEquals(2.0, r.center, DELTA)
        assertEquals(2.0, r.k, DELTA)
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
    fun `band merge approximates remote center and scale with unit weight`() {
        val stat = VarianceStat().band(k = 2.0)

        stat.merge(BandResult(center = 10.0, scale = 3.0, k = 1.0, lower = 7.0, upper = 13.0))
        val result = stat.read()

        assertEquals(10.0, result.center, DELTA)
        assertEquals(3.0, result.scale, DELTA)
        assertEquals(4.0, result.lower, DELTA)
        assertEquals(16.0, result.upper, DELTA)
    }

    @Test
    fun `merging an empty robust band leaves the destination unchanged`() {
        val stat = VarianceStat().band(k = 2.0).apply { update(7.0) }
        val before = stat.read()

        stat.merge(BandResult(Double.NaN, Double.NaN, 2.0, Double.NaN, Double.NaN))

        assertEquals(before, stat.read())
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

    @Test
    fun `band merge over a window lands at the caller timestamp`() {
        val t = 1_700_000_000_000_000_000L
        val stat = VarianceStat().band(k = 2.0).windowed(60.seconds)
        stat.merge(BandResult(center = 10.0, scale = 3.0, k = 1.0, lower = 7.0, upper = 13.0), t)
        assertEquals(10.0, stat.read(t).center, DELTA)
    }
}
