package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.DELTA
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class MaxStatTest {

    @Test
    fun `tracks maximum`() {
        val m = MaxStat()
        m.update(3.0)
        m.update(7.0)
        m.update(2.0)
        assertEquals(7.0, m.read().max, DELTA)
    }

    @Test
    fun `empty returns negative infinity`() {
        assertEquals(Double.NEGATIVE_INFINITY, MaxStat().read().max)
    }

    @Test
    fun `merge takes larger max`() {
        val m1 = MaxStat().apply { update(4.0) }
        val m2 = MaxStat().apply { update(9.0) }
        m1.merge(m2.read())
        assertEquals(9.0, m1.read().max, DELTA)
    }

    @Test
    fun `reset restores negative infinity`() {
        val m = MaxStat().apply { update(7.0) }
        m.reset()
        assertEquals(Double.NEGATIVE_INFINITY, m.read().max)
    }

    @Test
    fun `create produces fresh independent stat`() {
        val m1 = MaxStat().apply { update(5.0) }
        val m2 = m1.create()
        m2.update(10.0)
        assertEquals(5.0, m1.read().max, DELTA)
        assertEquals(10.0, m2.read().max, DELTA)
    }

    @Test
    fun `MaxStat ignores NaN inputs`() {
        val m = MaxStat()
        m.update(5.0)
        m.update(Double.NaN)
        assertEquals(5.0, m.read().max, DELTA)
    }

    @Test
    fun `MaxStat accepts positive infinity`() {
        val m = MaxStat()
        m.update(0.0)
        m.update(Double.POSITIVE_INFINITY)
        assertTrue(m.read().max.isInfinite() && m.read().max > 0.0)
    }
}
