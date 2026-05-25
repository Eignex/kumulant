package com.eignex.kumulant.stat.event

import kotlin.test.Test
import kotlin.test.assertEquals

private const val DELTA = 1e-12

class ExcursionStatTest {

    @Test
    fun `tracks peak and trough since peak`() {
        val s = ExcursionStat()
        listOf(1.0 to 100L, 5.0 to 200L, 3.0 to 300L, 2.0 to 400L).forEach { (v, t) ->
            s.update(v, t)
        }
        val r = s.read()
        assertEquals(5.0, r.peak, DELTA)
        assertEquals(200L, r.peakTimestampNanos)
        assertEquals(2.0, r.trough, DELTA)
        assertEquals(400L, r.troughTimestampNanos)
    }

    @Test
    fun `max excursion is monotonic across peaks`() {
        val s = ExcursionStat()
        listOf(10.0, 8.0, 5.0, 12.0, 11.0).forEach { s.update(it) }
        val r = s.read()
        assertEquals(12.0, r.peak, DELTA)
        assertEquals(11.0, r.trough, DELTA)
        assertEquals(5.0, r.maxExcursion, DELTA)
    }

    @Test
    fun `new peak resets the trough to that peak`() {
        val s = ExcursionStat()
        listOf(5.0, 3.0, 10.0).forEach { s.update(it) }
        val r = s.read()
        assertEquals(10.0, r.peak, DELTA)
        assertEquals(10.0, r.trough, DELTA)
    }

    @Test
    fun `current recovery is peak minus last value`() {
        val s = ExcursionStat()
        listOf(10.0, 5.0, 7.0).forEach { s.update(it) }
        val r = s.read()
        assertEquals(10.0 - 7.0, r.currentRecovery, DELTA)
    }

    @Test
    fun `single observation establishes peak and trough`() {
        val s = ExcursionStat()
        s.update(42.0, 100L)
        val r = s.read()
        assertEquals(42.0, r.peak, DELTA)
        assertEquals(42.0, r.trough, DELTA)
        assertEquals(0.0, r.maxExcursion, DELTA)
        assertEquals(0.0, r.currentRecovery, DELTA)
        assertEquals(100L, r.peakTimestampNanos)
    }

    @Test
    fun `read on a fresh stat returns zeros`() {
        val r = ExcursionStat().read()
        assertEquals(0.0, r.peak, DELTA)
        assertEquals(0.0, r.trough, DELTA)
        assertEquals(0.0, r.maxExcursion, DELTA)
        assertEquals(0.0, r.currentRecovery, DELTA)
    }

    @Test
    fun `reset clears state`() {
        val s = ExcursionStat().apply {
            update(10.0)
            update(2.0)
            update(5.0)
        }
        s.reset()
        s.update(7.0)
        val r = s.read()
        assertEquals(7.0, r.peak, DELTA)
        assertEquals(7.0, r.trough, DELTA)
        assertEquals(0.0, r.maxExcursion, DELTA)
    }
}
