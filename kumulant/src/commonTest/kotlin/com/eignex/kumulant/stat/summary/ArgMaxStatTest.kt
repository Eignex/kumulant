package com.eignex.kumulant.stat.summary

import kotlin.test.Test
import kotlin.test.assertEquals

private const val DELTA = 1e-12

class ArgMaxStatTest {

    @Test
    fun `tracks maximum and its timestamp`() {
        val m = ArgMaxStat()
        m.update(3.0, 10L)
        m.update(5.0, 20L)
        m.update(1.0, 30L)
        val r = m.read()
        assertEquals(5.0, r.max, DELTA)
        assertEquals(20L, r.atTimestampNanos)
    }

    @Test
    fun `tie keeps first occurrence`() {
        val m = ArgMaxStat()
        m.update(1.0, 10L)
        m.update(1.0, 20L)
        assertEquals(10L, m.read().atTimestampNanos)
    }

    @Test
    fun `empty returns negative infinity`() {
        val r = ArgMaxStat().read()
        assertEquals(Double.NEGATIVE_INFINITY, r.max)
        assertEquals(0L, r.atTimestampNanos)
    }

    @Test
    fun `merge keeps pair from larger max`() {
        val m1 = ArgMaxStat().apply { update(2.0, 10L) }
        val m2 = ArgMaxStat().apply { update(4.0, 20L) }
        m1.merge(m2.read())
        val r = m1.read()
        assertEquals(4.0, r.max, DELTA)
        assertEquals(20L, r.atTimestampNanos)
    }

    @Test
    fun `merge with smaller max keeps own pair`() {
        val m1 = ArgMaxStat().apply { update(4.0, 10L) }
        val m2 = ArgMaxStat().apply { update(2.0, 20L) }
        m1.merge(m2.read())
        val r = m1.read()
        assertEquals(4.0, r.max, DELTA)
        assertEquals(10L, r.atTimestampNanos)
    }

    @Test
    fun `merge with empty is a no-op`() {
        val m = ArgMaxStat().apply { update(2.0, 10L) }
        m.merge(ArgMaxStat().read())
        val r = m.read()
        assertEquals(2.0, r.max, DELTA)
        assertEquals(10L, r.atTimestampNanos)
    }

    @Test
    fun `reset restores infinity`() {
        val m = ArgMaxStat().apply { update(3.0, 10L) }
        m.reset()
        val r = m.read()
        assertEquals(Double.NEGATIVE_INFINITY, r.max)
        assertEquals(0L, r.atTimestampNanos)
    }

    @Test
    fun `create produces fresh independent stat`() {
        val m1 = ArgMaxStat().apply { update(1.0, 10L) }
        val m2 = m1.create()
        m2.update(5.0, 20L)
        assertEquals(1.0, m1.read().max, DELTA)
        assertEquals(5.0, m2.read().max, DELTA)
    }

    @Test
    fun `ignores NaN inputs`() {
        val m = ArgMaxStat()
        m.update(5.0, 10L)
        m.update(Double.NaN, 20L)
        val r = m.read()
        assertEquals(5.0, r.max, DELTA)
        assertEquals(10L, r.atTimestampNanos)
    }

    @Test
    fun `accepts positive infinity`() {
        val m = ArgMaxStat()
        m.update(0.0, 10L)
        m.update(Double.POSITIVE_INFINITY, 20L)
        val r = m.read()
        assertEquals(Double.POSITIVE_INFINITY, r.max)
        assertEquals(20L, r.atTimestampNanos)
    }
}
