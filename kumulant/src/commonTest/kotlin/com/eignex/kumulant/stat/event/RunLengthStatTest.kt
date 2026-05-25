package com.eignex.kumulant.stat.event

import kotlin.test.Test
import kotlin.test.assertEquals

class RunLengthStatTest {

    @Test
    fun `tracks current and longest run`() {
        val s = RunLengthStat()
        listOf(1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0).forEach { s.update(it) }
        val r = s.read()
        assertEquals(1L, r.current)
        assertEquals(3L, r.longest)
    }

    @Test
    fun `zero resets the current run`() {
        val s = RunLengthStat()
        listOf(1.0, 1.0, 0.0, 1.0).forEach { s.update(it) }
        val r = s.read()
        assertEquals(1L, r.current)
        assertEquals(2L, r.longest)
    }

    @Test
    fun `NaN counts as falsy and resets the current run`() {
        val s = RunLengthStat()
        listOf(1.0, 1.0, Double.NaN, 1.0).forEach { s.update(it) }
        val r = s.read()
        assertEquals(1L, r.current)
        assertEquals(2L, r.longest)
    }

    @Test
    fun `negative non-zero values are truthy`() {
        val s = RunLengthStat()
        listOf(-1.0, -2.0, -3.0).forEach { s.update(it) }
        val r = s.read()
        assertEquals(3L, r.current)
        assertEquals(3L, r.longest)
    }

    @Test
    fun `reset clears state`() {
        val s = RunLengthStat().apply {
            update(1.0)
            update(1.0)
        }
        s.reset()
        val r = s.read()
        assertEquals(0L, r.current)
        assertEquals(0L, r.longest)
    }

    @Test
    fun `read on a fresh stat returns zeros`() {
        val r = RunLengthStat().read()
        assertEquals(0L, r.current)
        assertEquals(0L, r.longest)
    }
}
