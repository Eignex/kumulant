package com.eignex.kumulant.stat.summary

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private const val DELTA = 1e-12

class MinStatTest {

    @Test
    fun `tracks minimum`() {
        val m = MinStat()
        m.update(3.0)
        m.update(1.0)
        m.update(5.0)
        assertEquals(1.0, m.read().min, DELTA)
    }

    @Test
    fun `empty returns positive infinity`() {
        assertEquals(Double.POSITIVE_INFINITY, MinStat().read().min)
    }

    @Test
    fun `merge takes smaller min`() {
        val m1 = MinStat().apply { update(4.0) }
        val m2 = MinStat().apply { update(2.0) }
        m1.merge(m2.read())
        assertEquals(2.0, m1.read().min, DELTA)
    }

    @Test
    fun `reset restores infinity`() {
        val m = MinStat().apply { update(3.0) }
        m.reset()
        assertEquals(Double.POSITIVE_INFINITY, m.read().min)
    }

    @Test
    fun `create produces fresh independent stat`() {
        val m1 = MinStat().apply { update(5.0) }
        val m2 = m1.create()
        m2.update(1.0)
        assertEquals(5.0, m1.read().min, DELTA)
        assertEquals(1.0, m2.read().min, DELTA)
    }

    @Test
    fun `MinStat ignores NaN inputs`() {
        val m = MinStat()
        m.update(5.0)
        m.update(Double.NaN)
        assertEquals(5.0, m.read().min, DELTA)
    }

    @Test
    fun `MinStat accepts negative infinity`() {
        val m = MinStat()
        m.update(0.0)
        m.update(Double.NEGATIVE_INFINITY)
        assertTrue(m.read().min.isInfinite() && m.read().min < 0.0)
    }
}
