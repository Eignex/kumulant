package com.eignex.kumulant.stat.forecast

import com.eignex.kumulant.DELTA
import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class SeasonalSmoothingStatTest {

    @Test
    fun `additive seasonal recovers a synthetic seasonal pattern`() {
        val s = SeasonalSmoothingStat(alpha = 0.4, beta = 0.05, gamma = 0.6, period = 4)
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
        val s = SeasonalSmoothingStat(
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
    fun `period less than two is rejected`() {
        assertFailsWith<IllegalArgumentException> {
            SeasonalSmoothingStat(alpha = 0.1, beta = 0.1, gamma = 0.1, period = 1)
        }
    }

    @Test
    fun `forecast wraps around the seasonal vector`() {
        val r = SeasonalSmoothingResult(
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
                SeasonalSmoothingResult(
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
        val s = SeasonalSmoothingStat(alpha = 0.3, beta = 0.2, gamma = 0.1, period = 4)
        s.update(1.0)
        s.merge(
            SeasonalSmoothingResult(
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
}

class SeasonalForecastHorizonTest {

    @Test
    fun `a huge horizon picks the same slot as the equivalent short one`() {
        val s = SeasonalSmoothingStat(period = 3, alpha = 0.5, beta = 0.0, gamma = 0.5)
        for (i in 0 until 29) s.update(1.0 + (i % 3))
        val r = s.read()
        assertEquals(r.forecast(1), r.forecast(Int.MAX_VALUE), DELTA)
    }
}
