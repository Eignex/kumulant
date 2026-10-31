package com.eignex.kumulant.stat.forecast

import com.eignex.kumulant.DELTA
import kotlin.math.abs
import kotlin.math.exp
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class ExponentialSmoothingStatTest {

    @Test
    fun `first update seeds level and zero trend`() {
        val s = ExponentialSmoothingStat(alpha = 0.5, beta = 0.2)
        s.update(42.0)
        val r = s.read()
        assertEquals(42.0, r.level, DELTA)
        assertEquals(0.0, r.trend, DELTA)
    }

    @Test
    fun `tracks linear trend on a straight-line input`() {
        val s = ExponentialSmoothingStat(alpha = 0.5, beta = 0.5)
        for (i in 0..50) s.update(i.toDouble())
        val r = s.read()
        assertTrue(r.trend in 0.9..1.1, "trend=${r.trend}")
    }

    @Test
    fun `without seasonality the single season stays at its identity`() {
        for ((mode, identity) in listOf(SeasonalMode.Additive to 0.0, SeasonalMode.Multiplicative to 1.0)) {
            val s = ExponentialSmoothingStat(alpha = 0.5, beta = 0.2, mode = mode)
            for (v in listOf(3.0, 5.0, 4.0, 8.0)) s.update(v)
            assertEquals(listOf(identity), s.read().seasons, "mode=$mode")
        }
    }

    @Test
    fun `forecast steps zero returns the level`() {
        val s = ExponentialSmoothingStat(alpha = 0.3, beta = 0.1).apply {
            for (v in listOf(1.0, 2.0, 3.0, 4.0)) update(v)
        }
        val r = s.read()
        assertEquals(r.level, r.forecast(0), DELTA)
    }

    @Test
    fun `undamped forecast equals level plus steps times trend`() {
        assertEquals(10.0 + 3.0 * 0.5, trendOnly(level = 10.0, trend = 0.5, phi = 1.0).forecast(3), DELTA)
    }

    @Test
    fun `damped forecast geometrically discounts the trend`() {
        assertEquals(0.875, trendOnly(level = 0.0, trend = 1.0, phi = 0.5).forecast(3), DELTA)
    }

    @Test
    fun `phi out of range is rejected`() {
        assertFailsWith<IllegalArgumentException> { ExponentialSmoothingStat(alpha = 0.1, phi = 0.0) }
        assertFailsWith<IllegalArgumentException> { ExponentialSmoothingStat(alpha = 0.1, phi = 1.1) }
    }

    @Test
    fun `negative forecast steps are rejected`() {
        assertFailsWith<IllegalArgumentException> { trendOnly(level = 1.0, trend = 1.0, phi = 1.0).forecast(-1) }
    }

    @Test
    fun `weight scales the per-update smoothing`() {
        val s = ExponentialSmoothingStat(alpha = 0.5, beta = 0.0)
        s.update(0.0)
        s.update(value = 100.0, weight = 1.0)
        val expectedMove = 100.0 * (1.0 - exp(-0.5))
        assertEquals(expectedMove, s.read().level, 1e-3)
    }

    @Test
    fun `additive seasonal recovers a synthetic seasonal pattern`() {
        val s = ExponentialSmoothingStat(alpha = 0.4, beta = 0.05, gamma = 0.6, period = 4)
        val baseline = 10.0
        val pattern = doubleArrayOf(2.0, -1.0, -2.0, 1.0)
        repeat(80) { i ->
            s.update(baseline + pattern[i % 4])
        }
        val r = s.read()
        assertEquals(baseline, r.level, 0.5)
        val seasonSum = r.seasons.sum()
        assertTrue(abs(seasonSum) < 1.0, "seasonal factors should average near zero, got sum=$seasonSum")
    }

    @Test
    fun `multiplicative seasonal recovers a multiplicative pattern`() {
        val s = ExponentialSmoothingStat(
            alpha = 0.4,
            beta = 0.05,
            gamma = 0.6,
            period = 3,
            mode = SeasonalMode.Multiplicative,
        )
        val baseline = 10.0
        val pattern = doubleArrayOf(1.2, 0.9, 0.9)
        repeat(90) { i ->
            s.update(baseline * pattern[i % 3])
        }
        val r = s.read()
        assertEquals(baseline, r.level, 0.5)
        val seasonProduct = r.seasons.fold(1.0) { acc, x -> acc * x }
        assertTrue(abs(seasonProduct - 1.0) < 0.2, "seasonal factor product=$seasonProduct")
    }

    @Test
    fun `period below one is rejected`() {
        assertFailsWith<IllegalArgumentException> { ExponentialSmoothingStat(alpha = 0.1, period = 0) }
    }

    @Test
    fun `forecast wraps around the seasonal vector`() {
        val r = ExponentialSmoothingResult(
            level = 10.0,
            trend = 0.0,
            seasons = listOf(1.0, 2.0, 3.0, 4.0),
            currentSlot = 0,
            phi = 1.0,
            mode = SeasonalMode.Additive,
        )
        assertEquals(11.0, r.forecast(1), DELTA)
        assertEquals(11.0, r.forecast(5), DELTA)
        assertEquals(13.0, r.forecast(3), DELTA)
    }

    @Test
    fun `a result rejects a slot that does not index its seasons`() {
        for (slot in listOf(-1, 4, 99)) {
            assertFailsWith<IllegalArgumentException> {
                ExponentialSmoothingResult(
                    level = 1.0,
                    trend = 0.0,
                    seasons = listOf(0.0, 0.0, 0.0, 0.0),
                    currentSlot = slot,
                    phi = 1.0,
                    mode = SeasonalMode.Additive,
                )
            }
        }
    }

    @Test
    fun `merging a decoded result leaves the stat updatable`() {
        val s = ExponentialSmoothingStat(alpha = 0.3, beta = 0.2, gamma = 0.1, period = 4)
        s.update(1.0)
        s.merge(
            ExponentialSmoothingResult(
                level = 1.0,
                trend = 0.0,
                seasons = listOf(0.0, 0.0, 0.0, 0.0),
                currentSlot = 3,
                phi = 1.0,
                mode = SeasonalMode.Additive,
            ),
        )
        s.update(2.0)
        assertTrue(s.read().level.isFinite())
    }

    @Test
    fun `a retraction leaves level and trend unchanged`() {
        val s = ExponentialSmoothingStat(alpha = 0.5, beta = 0.5)
        s.update(0.0)
        s.update(10.0)
        val before = s.read()
        s.update(5.0, weight = -1.0)
        assertEquals(before, s.read())
    }

    @Test
    fun `a retraction does not advance the seasonal slot`() {
        val s = ExponentialSmoothingStat(alpha = 0.5, beta = 0.5, gamma = 0.5, period = 3)
        listOf(1.0, 5.0, 9.0, 1.0).forEach { s.update(it) }
        val before = s.read()
        s.update(5.0, weight = -1.0)
        assertEquals(before.currentSlot, s.read().currentSlot)
    }

    @Test
    fun `merge rejects a snapshot with a different seasonal mode`() {
        val additive = ExponentialSmoothingStat(alpha = 0.5, period = 3)
        val multiplicative = ExponentialSmoothingStat(alpha = 0.5, period = 3, mode = SeasonalMode.Multiplicative)
        listOf(1.0, 2.0, 3.0).forEach { multiplicative.update(it) }
        assertFailsWith<IllegalArgumentException> { additive.merge(multiplicative.read()) }
    }
}

class SeasonalForecastHorizonTest {

    @Test
    fun `a huge horizon picks the same slot as the equivalent short one`() {
        val s = ExponentialSmoothingStat(period = 3, alpha = 0.5, beta = 0.0, gamma = 0.5)
        for (i in 0 until 29) s.update(1.0 + (i % 3))
        val r = s.read()
        assertEquals(r.forecast(1), r.forecast(Int.MAX_VALUE), DELTA)
    }
}

private fun trendOnly(level: Double, trend: Double, phi: Double) = ExponentialSmoothingResult(
    level = level,
    trend = trend,
    seasons = listOf(0.0),
    currentSlot = 0,
    phi = phi,
    mode = SeasonalMode.Additive,
)
