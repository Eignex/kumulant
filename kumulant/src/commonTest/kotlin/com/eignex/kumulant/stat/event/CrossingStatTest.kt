package com.eignex.kumulant.stat.event

import kotlin.test.Test
import kotlin.test.assertEquals

class CrossingStatTest {

    @Test
    fun `counts up-crossings of the configured level`() {
        val s = CrossingStat(level = 5.0)
        listOf(0.0, 3.0, 5.0, 10.0, 4.0, 6.0).forEach { s.update(it) }
        val r = s.read()
        assertEquals(2L, r.upCrossings)
        assertEquals(1L, r.downCrossings)
    }

    @Test
    fun `first update establishes the baseline and is not counted`() {
        val s = CrossingStat(level = 0.0)
        s.update(10.0)
        s.update(20.0)
        val r = s.read()
        assertEquals(0L, r.upCrossings)
        assertEquals(0L, r.downCrossings)
    }

    @Test
    fun `repeated values on the same side do not count`() {
        val s = CrossingStat(level = 0.0)
        listOf(-1.0, -2.0, -3.0, 1.0, 2.0, 3.0).forEach { s.update(it) }
        val r = s.read()
        assertEquals(1L, r.upCrossings)
        assertEquals(0L, r.downCrossings)
    }

    @Test
    fun `boundary value counts as the at-or-above side`() {
        val s = CrossingStat(level = 5.0)
        s.update(4.0)
        s.update(5.0)
        s.update(4.0)
        val r = s.read()
        assertEquals(1L, r.upCrossings)
        assertEquals(1L, r.downCrossings)
    }

    @Test
    fun `merge sums crossing counts`() {
        val a = CrossingStat(level = 0.0).apply {
            update(-1.0)
            update(1.0)
            update(-1.0)
        }
        val b = CrossingStat(level = 0.0).apply {
            update(-2.0)
            update(2.0)
        }
        a.merge(b.read())
        val r = a.read()
        assertEquals(2L, r.upCrossings)
        assertEquals(1L, r.downCrossings)
    }

    @Test
    fun `reset clears the counts and baseline`() {
        val s = CrossingStat(level = 0.0).apply {
            update(-1.0)
            update(1.0)
            update(-1.0)
        }
        s.reset()
        s.update(100.0)
        s.update(-100.0)
        val r = s.read()
        assertEquals(0L, r.upCrossings)
        assertEquals(1L, r.downCrossings)
    }

    @Test
    fun `result carries the configured level`() {
        val r = CrossingStat(level = 3.14).read()
        assertEquals(3.14, r.level)
    }
}
