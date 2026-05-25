package com.eignex.kumulant.stat.event

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class RecencyStatTest {

    @Test
    fun `read before any update reports no observation`() {
        val s = RecencyStat()
        val r = s.read(timestampNanos = 1_000L)
        assertFalse(r.hasObservation)
        assertEquals(-1L, r.elapsedNanos)
    }

    @Test
    fun `elapsed is read timestamp minus last observation`() {
        val s = RecencyStat()
        s.update(value = 0.0, timestampNanos = 500L)
        val r = s.read(timestampNanos = 2_500L)
        assertTrue(r.hasObservation)
        assertEquals(500L, r.lastObservedTimestampNanos)
        assertEquals(2_000L, r.elapsedNanos)
    }

    @Test
    fun `later observation overwrites earlier`() {
        val s = RecencyStat()
        s.update(value = 0.0, timestampNanos = 100L)
        s.update(value = 0.0, timestampNanos = 5_000L)
        val r = s.read(timestampNanos = 6_000L)
        assertEquals(5_000L, r.lastObservedTimestampNanos)
        assertEquals(1_000L, r.elapsedNanos)
    }

    @Test
    fun `out-of-order observation does not move the cell backwards`() {
        val s = RecencyStat()
        s.update(value = 0.0, timestampNanos = 5_000L)
        s.update(value = 0.0, timestampNanos = 1_000L)
        val r = s.read(timestampNanos = 6_000L)
        assertEquals(5_000L, r.lastObservedTimestampNanos)
    }

    @Test
    fun `merge takes the latest observation`() {
        val a = RecencyStat().apply { update(value = 0.0, timestampNanos = 1_000L) }
        val b = RecencyStat().apply { update(value = 0.0, timestampNanos = 7_000L) }
        a.merge(b.read(7_000L))
        assertEquals(7_000L, a.read(8_000L).lastObservedTimestampNanos)
    }

    @Test
    fun `merge from empty replica is a no-op`() {
        val a = RecencyStat().apply { update(value = 0.0, timestampNanos = 1_000L) }
        val empty = RecencyStat()
        a.merge(empty.read(2_000L))
        val r = a.read(3_000L)
        assertEquals(1_000L, r.lastObservedTimestampNanos)
        assertTrue(r.hasObservation)
    }

    @Test
    fun `reset clears state`() {
        val s = RecencyStat().apply { update(value = 0.0, timestampNanos = 1_000L) }
        s.reset()
        val r = s.read(timestampNanos = 5_000L)
        assertFalse(r.hasObservation)
        assertEquals(-1L, r.elapsedNanos)
    }

    @Test
    fun `create produces an independent stat`() {
        val tpl = RecencyStat().apply { update(value = 0.0, timestampNanos = 100L) }
        val fresh = tpl.create()
        fresh.update(value = 0.0, timestampNanos = 5_000L)
        assertEquals(100L, tpl.read(10_000L).lastObservedTimestampNanos)
        assertEquals(5_000L, fresh.read(10_000L).lastObservedTimestampNanos)
    }
}
