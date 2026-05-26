package com.eignex.kumulant.stat.forecast

import kotlin.math.exp
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

private const val DELTA = 1e-9

class HoltStatTest {

    @Test
    fun `first update seeds level and zero trend`() {
        val s = HoltStat(alpha = 0.5, beta = 0.2)
        s.update(42.0)
        val r = s.read()
        assertEquals(42.0, r.level, DELTA)
        assertEquals(0.0, r.trend, DELTA)
    }

    @Test
    fun `tracks linear trend on a straight-line input`() {
        val s = HoltStat(alpha = 0.5, beta = 0.5)
        for (i in 0..50) s.update(i.toDouble())
        val r = s.read()
        // On a perfect 1-per-step line, the smoothed trend should be near 1.0.
        assertTrue(r.trend in 0.9..1.1, "trend=${r.trend}")
    }

    @Test
    fun `forecast steps zero returns the level`() {
        val s = HoltStat(alpha = 0.3, beta = 0.1).apply {
            for (v in listOf(1.0, 2.0, 3.0, 4.0)) update(v)
        }
        val r = s.read()
        assertEquals(r.level, r.forecast(0), DELTA)
    }

    @Test
    fun `undamped forecast equals level plus steps times trend`() {
        val r = HoltResult(level = 10.0, trend = 0.5, phi = 1.0)
        assertEquals(10.0 + 3.0 * 0.5, r.forecast(3), DELTA)
    }

    @Test
    fun `damped forecast geometrically discounts the trend`() {
        val r = HoltResult(level = 0.0, trend = 1.0, phi = 0.5)
        // phi + phi^2 + phi^3 = 0.5 + 0.25 + 0.125 = 0.875
        assertEquals(0.875, r.forecast(3), DELTA)
    }

    @Test
    fun `phi out of range is rejected`() {
        assertFailsWith<IllegalArgumentException> { HoltStat(alpha = 0.1, beta = 0.1, phi = 0.0) }
        assertFailsWith<IllegalArgumentException> { HoltStat(alpha = 0.1, beta = 0.1, phi = 1.1) }
    }

    @Test
    fun `negative forecast steps are rejected`() {
        val r = HoltResult(level = 1.0, trend = 1.0, phi = 1.0)
        assertFailsWith<IllegalArgumentException> { r.forecast(-1) }
    }

    @Test
    fun `weight scales the per-update smoothing`() {
        val s = HoltStat(alpha = 0.5, beta = 0.0)
        s.update(0.0)
        s.update(value = 100.0, weight = 1.0)
        // The level should move from 0 toward 100 by the correction factor 1 - exp(-0.5 * 1).
        val r = s.read()
        val expectedMove = 100.0 * (1.0 - exp(-0.5))
        assertEquals(expectedMove, r.level, 1e-3)
    }

    @Test
    fun `reset clears state`() {
        val s = HoltStat(alpha = 0.5, beta = 0.5).apply {
            for (v in listOf(1.0, 2.0, 3.0)) update(v)
        }
        s.reset()
        val r = s.read()
        assertEquals(0.0, r.level, DELTA)
        assertEquals(0.0, r.trend, DELTA)
    }

    @Test
    fun `create produces an independent stat`() {
        val template = HoltStat(alpha = 0.5, beta = 0.5).apply {
            update(1.0)
            update(2.0)
        }
        val fresh = template.create()
        fresh.update(10.0)
        val tr = template.read()
        val fr = fresh.read()
        assertTrue(tr.level != fr.level)
    }
}
