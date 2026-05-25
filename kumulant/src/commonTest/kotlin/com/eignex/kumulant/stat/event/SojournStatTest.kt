package com.eignex.kumulant.stat.event

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SojournStatTest {

    private val states = listOf(0L, 1L, 2L)

    @Test
    fun `read before any update reports no state`() {
        val s = SojournStat(states)
        val r = s.read(timestampNanos = 100L)
        assertFalse(r.hasState)
        for (t in r.totalNanosByState) assertEquals(0L, t)
        for (t in r.transitionsByState) assertEquals(0L, t)
    }

    @Test
    fun `first update sets current state and counts the entry transition`() {
        val s = SojournStat(states)
        s.update(value = 1L, timestampNanos = 1_000L)
        val r = s.read(timestampNanos = 2_000L)
        assertTrue(r.hasState)
        assertEquals(1L, r.currentState)
        assertEquals(1_000L, r.currentStateEnterTimestampNanos)
        assertEquals(1_000L, r.currentDwellNanos)
        assertEquals(listOf(0L, 1L, 0L), r.transitionsByState)
    }

    @Test
    fun `state change adds elapsed nanos to the prior state`() {
        val s = SojournStat(states)
        s.update(value = 0L, timestampNanos = 0L)
        s.update(value = 1L, timestampNanos = 3_000L) // 3000ns in state 0
        s.update(value = 0L, timestampNanos = 5_000L) // 2000ns in state 1
        val r = s.read(timestampNanos = 9_000L) // 4000ns in state 0 so far
        assertEquals(listOf(3_000L, 2_000L, 0L), r.totalNanosByState)
        assertEquals(listOf(2L, 1L, 0L), r.transitionsByState)
        assertEquals(0L, r.currentState)
        assertEquals(4_000L, r.currentDwellNanos)
    }

    @Test
    fun `same-state update does not add a transition or interrupt dwell`() {
        val s = SojournStat(states)
        s.update(value = 2L, timestampNanos = 100L)
        s.update(value = 2L, timestampNanos = 500L)
        val r = s.read(timestampNanos = 1_000L)
        assertEquals(listOf(0L, 0L, 1L), r.transitionsByState)
        assertEquals(0L, r.totalNanosByState[2])
        assertEquals(900L, r.currentDwellNanos)
    }

    @Test
    fun `unknown state is rejected`() {
        val s = SojournStat(states)
        assertFailsWith<IllegalArgumentException> { s.update(value = 99L, timestampNanos = 0L) }
    }

    @Test
    fun `duplicate states are rejected`() {
        assertFailsWith<IllegalArgumentException> { SojournStat(listOf(0L, 1L, 0L)) }
    }

    @Test
    fun `empty alphabet is rejected`() {
        assertFailsWith<IllegalArgumentException> { SojournStat(emptyList()) }
    }

    @Test
    fun `merge combines totals and picks later current state`() {
        val a = SojournStat(states).apply {
            update(value = 0L, timestampNanos = 0L)
            update(value = 1L, timestampNanos = 1_000L)
        }
        val b = SojournStat(states).apply {
            update(value = 2L, timestampNanos = 500L)
            update(value = 0L, timestampNanos = 2_500L)
        }
        a.merge(b.read(timestampNanos = 3_000L))
        val r = a.read(timestampNanos = 3_500L)
        // Local: state 0 1000ns + remote state 2 2000ns merged elsewhere.
        // Remote held: state 2 -> 2000ns, currently in state 0 entered at 2500.
        assertEquals(1_000L + 0L, r.totalNanosByState[0])
        assertEquals(0L, r.totalNanosByState[1])
        assertEquals(2_000L, r.totalNanosByState[2])
        assertEquals(0L, r.currentState)
        assertEquals(2_500L, r.currentStateEnterTimestampNanos)
    }

    @Test
    fun `reset clears everything`() {
        val s = SojournStat(states).apply {
            update(value = 0L, timestampNanos = 0L)
            update(value = 1L, timestampNanos = 1_000L)
        }
        s.reset()
        val r = s.read(timestampNanos = 2_000L)
        assertFalse(r.hasState)
        for (t in r.totalNanosByState) assertEquals(0L, t)
        for (t in r.transitionsByState) assertEquals(0L, t)
    }

    @Test
    fun `create produces an independent stat with the same alphabet`() {
        val tpl = SojournStat(states).apply { update(value = 0L, timestampNanos = 0L) }
        val fresh = tpl.create()
        fresh.update(value = 1L, timestampNanos = 500L)
        assertEquals(0L, tpl.read(1_000L).currentState)
        assertEquals(1L, fresh.read(1_000L).currentState)
    }
}
