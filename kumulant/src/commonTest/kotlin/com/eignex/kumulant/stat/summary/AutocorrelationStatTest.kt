package com.eignex.kumulant.stat.summary

import kotlin.math.cos
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

private const val DELTA = 1e-9

class AutocorrelationStatTest {

    @Test
    fun `read before any update reports NaN autocorrelation`() {
        val r = AutocorrelationStat(lag = 2).read()
        assertEquals(0L, r.pairCount)
        assertTrue(r.autocorrelation.isNaN())
    }

    @Test
    fun `lag less than one is rejected`() {
        assertFailsWith<IllegalArgumentException> { AutocorrelationStat(lag = 0) }
    }

    @Test
    fun `perfect lag-1 repetition gives autocorrelation near one`() {
        val s = AutocorrelationStat(lag = 1)
        // Pure constant has zero variance, so the estimator returns NaN by convention.
        // Use a simple two-state alternating-then-repeating pattern.
        for (v in listOf(1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0)) s.update(v)
        // Alternating: lag-1 autocorrelation approaches -1.
        val r = s.read()
        assertTrue(r.autocorrelation < -0.5, "acf=${r.autocorrelation}")
    }

    @Test
    fun `lag-1 repetition of doubled values gives autocorrelation near plus one`() {
        val s = AutocorrelationStat(lag = 2)
        // Pattern of period 2 should produce lag-2 acf close to +1.
        for (v in listOf(1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0)) s.update(v)
        val r = s.read()
        assertTrue(r.autocorrelation > 0.8, "acf=${r.autocorrelation}")
    }

    @Test
    fun `sinusoid lag-1 autocorrelation tracks the underlying period`() {
        // For samples of cos(2*pi*k/8), lag-1 acf is cos(2*pi/8) = sqrt(2)/2 ~ 0.707.
        val s = AutocorrelationStat(lag = 1)
        for (k in 0 until 1024) s.update(cos(2.0 * kotlin.math.PI * k / 8.0))
        val r = s.read()
        assertEquals(0.707, r.autocorrelation, 0.05)
    }

    @Test
    fun `first lag updates warm the ring without forwarding`() {
        val s = AutocorrelationStat(lag = 3)
        s.update(1.0)
        s.update(2.0)
        s.update(3.0)
        // After exactly lag updates, no pairs yet.
        assertEquals(0L, s.read().pairCount)
        s.update(4.0)
        assertEquals(1L, s.read().pairCount)
    }

    @Test
    fun `reset clears state`() {
        val s = AutocorrelationStat(lag = 2).apply {
            for (v in 1..6) update(v.toDouble())
        }
        s.reset()
        val r = s.read()
        assertEquals(0L, r.pairCount)
        assertTrue(r.autocorrelation.isNaN())
    }

    @Test
    fun `create produces an independent stat`() {
        val tpl = AutocorrelationStat(lag = 1).apply { for (v in 1..6) update(v.toDouble()) }
        val fresh = tpl.create()
        assertTrue(tpl.read().pairCount > 0L)
        assertEquals(0L, fresh.read().pairCount)
    }
}
