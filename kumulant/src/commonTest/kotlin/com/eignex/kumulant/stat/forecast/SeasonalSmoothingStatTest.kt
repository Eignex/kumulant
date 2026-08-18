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
        // Seasonal factors should be close to the pattern up to a phase rotation; check the sum is ~0.
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
        // For a multiplicative cycle of length 3 the product of the factors should be close to 1.
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
        // Step 1 hits seasons[0] = 1.0 (no trend), step 5 wraps to the same slot.
        assertEquals(11.0, r.forecast(1), DELTA)
        assertEquals(11.0, r.forecast(5), DELTA)
        assertEquals(13.0, r.forecast(3), DELTA)
    }

    @Test
    fun `a result rejects a slot that does not index its seasons`() {
        // The slot indexes the seasons array on the stat's update path, so an unchecked one merges
        // without complaint and then takes the next update out with an index-out-of-bounds. Rejecting
        // it at the result keeps that off the update path entirely.
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

    @Test
    fun `reset clears state`() {
        val s = SeasonalSmoothingStat(alpha = 0.4, beta = 0.1, gamma = 0.5, period = 4)
        repeat(20) { s.update(it.toDouble()) }
        s.reset()
        val r = s.read()
        assertEquals(0.0, r.level, DELTA)
        assertEquals(0.0, r.trend, DELTA)
        for (sx in r.seasons) assertEquals(0.0, sx, DELTA)
    }
}
