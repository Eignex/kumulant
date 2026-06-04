package com.eignex.kumulant.stat.summary

import kotlin.test.Test
import kotlin.test.assertEquals

private const val DELTA = 1e-12

class ArgMinStatTest {

    @Test
    fun `tracks minimum and its timestamp`() {
        val m = ArgMinStat()
        m.update(3.0, 10L)
        m.update(1.0, 20L)
        m.update(5.0, 30L)
        val r = m.read()
        assertEquals(1.0, r.min, DELTA)
        assertEquals(20L, r.atTimestampNanos)
    }

    @Test
    fun `tie keeps first occurrence`() {
        val m = ArgMinStat()
        m.update(1.0, 10L)
        m.update(1.0, 20L)
        assertEquals(10L, m.read().atTimestampNanos)
    }

    @Test
    fun `empty returns positive infinity`() {
        val r = ArgMinStat().read()
        assertEquals(Double.POSITIVE_INFINITY, r.min)
        assertEquals(0L, r.atTimestampNanos)
    }

    @Test
    fun `merge keeps pair from smaller min`() {
        val m1 = ArgMinStat().apply { update(4.0, 10L) }
        val m2 = ArgMinStat().apply { update(2.0, 20L) }
        m1.merge(m2.read())
        val r = m1.read()
        assertEquals(2.0, r.min, DELTA)
        assertEquals(20L, r.atTimestampNanos)
    }

    @Test
    fun `merge with larger min keeps own pair`() {
        val m1 = ArgMinStat().apply { update(2.0, 10L) }
        val m2 = ArgMinStat().apply { update(4.0, 20L) }
        m1.merge(m2.read())
        val r = m1.read()
        assertEquals(2.0, r.min, DELTA)
        assertEquals(10L, r.atTimestampNanos)
    }

    @Test
    fun `merge with empty is a no-op`() {
        val m = ArgMinStat().apply { update(2.0, 10L) }
        m.merge(ArgMinStat().read())
        val r = m.read()
        assertEquals(2.0, r.min, DELTA)
        assertEquals(10L, r.atTimestampNanos)
    }

    @Test
    fun `reset restores infinity`() {
        val m = ArgMinStat().apply { update(3.0, 10L) }
        m.reset()
        val r = m.read()
        assertEquals(Double.POSITIVE_INFINITY, r.min)
        assertEquals(0L, r.atTimestampNanos)
    }

    @Test
    fun `create produces fresh independent stat`() {
        val m1 = ArgMinStat().apply { update(5.0, 10L) }
        val m2 = m1.create()
        m2.update(1.0, 20L)
        assertEquals(5.0, m1.read().min, DELTA)
        assertEquals(1.0, m2.read().min, DELTA)
    }

    @Test
    fun `ignores NaN inputs`() {
        val m = ArgMinStat()
        m.update(5.0, 10L)
        m.update(Double.NaN, 20L)
        val r = m.read()
        assertEquals(5.0, r.min, DELTA)
        assertEquals(10L, r.atTimestampNanos)
    }

    @Test
    fun `accepts negative infinity`() {
        val m = ArgMinStat()
        m.update(0.0, 10L)
        m.update(Double.NEGATIVE_INFINITY, 20L)
        val r = m.read()
        assertEquals(Double.NEGATIVE_INFINITY, r.min)
        assertEquals(20L, r.atTimestampNanos)
    }
}
