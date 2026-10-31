package com.eignex.kumulant.stat.forecast

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ForecastMergeTest {

    @Test
    fun `merging a seasonal trace brings its phase with its factors`() {
        val a = ExponentialSmoothingStat(alpha = 0.3, beta = 0.2, gamma = 0.1, period = 4)
        val b = ExponentialSmoothingStat(alpha = 0.3, beta = 0.2, gamma = 0.1, period = 4)
        repeat(9) { a.update(it.toDouble()) }
        repeat(6) { b.update(it.toDouble()) }
        val target = b.read().currentSlot

        a.merge(b.read())

        assertEquals(target, a.read().currentSlot)
    }

    @Test
    fun `a damped forecast far ahead converges instead of stalling`() {
        val stat = ExponentialSmoothingStat(alpha = 0.5, beta = 0.5, phi = 0.9)
        stat.update(1.0)
        stat.update(2.0)
        val r = stat.read()

        val far = r.forecast(Int.MAX_VALUE)
        assertTrue(far.isFinite(), "damped forecast diverged to $far")
        assertEquals(r.level + (0.9 / 0.1) * r.trend, far, 1e-6)
    }

    @Test
    fun `an undamped forecast is still linear in the horizon`() {
        val stat = ExponentialSmoothingStat(alpha = 0.5, beta = 0.5)
        stat.update(1.0)
        stat.update(2.0)
        val r = stat.read()

        assertEquals(r.level + 10.0 * r.trend, r.forecast(10), 1e-9)
    }

    @Test
    fun `a seasonal forecast far ahead stays finite`() {
        val stat = ExponentialSmoothingStat(alpha = 0.3, beta = 0.2, gamma = 0.1, period = 4, phi = 0.9)
        repeat(12) { stat.update(it.toDouble()) }

        assertTrue(stat.read().forecast(1_000_000).isFinite())
    }
}
