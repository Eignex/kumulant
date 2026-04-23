package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.AtomicMode
import com.eignex.kumulant.concurrent.SerialMode
import kotlin.test.Test
import kotlin.test.assertEquals

private const val DELTA = 1e-12

class RangeStatsTest {

    @Test
    fun `tracks min and max`() {
        val r = Range()
        r.update(3.0)
        r.update(1.0)
        r.update(5.0)
        r.update(2.0)
        assertEquals(1.0, r.read().min, DELTA)
        assertEquals(5.0, r.read().max, DELTA)
    }

    @Test
    fun `single value makes min equal max`() {
        val r = Range()
        r.update(42.0)
        assertEquals(42.0, r.read().min, DELTA)
        assertEquals(42.0, r.read().max, DELTA)
    }

    @Test
    fun `negative values`() {
        val r = Range()
        r.update(-5.0)
        r.update(-10.0)
        r.update(-1.0)
        assertEquals(-10.0, r.read().min, DELTA)
        assertEquals(-1.0, r.read().max, DELTA)
    }

    @Test
    fun `empty stat returns infinities`() {
        val r = Range()
        val result = r.read()
        assertEquals(Double.POSITIVE_INFINITY, result.min)
        assertEquals(Double.NEGATIVE_INFINITY, result.max)
    }

    @Test
    fun `merge combines ranges`() {
        val r1 = Range().apply {
            update(2.0)
            update(8.0)
        }
        val r2 = Range().apply {
            update(1.0)
            update(5.0)
        }
        r1.merge(r2.read())
        assertEquals(1.0, r1.read().min, DELTA)
        assertEquals(8.0, r1.read().max, DELTA)
    }

    @Test
    fun `merge with empty other is no-op`() {
        val r1 = Range().apply {
            update(3.0)
            update(7.0)
        }
        val r2 = Range()
        r1.merge(r2.read())
        assertEquals(3.0, r1.read().min, DELTA)
        assertEquals(7.0, r1.read().max, DELTA)
    }

    @Test
    fun `reset clears state`() {
        val r = Range().apply {
            update(1.0)
            update(9.0)
        }
        r.reset()
        assertEquals(Double.POSITIVE_INFINITY, r.read().min)
        assertEquals(Double.NEGATIVE_INFINITY, r.read().max)
    }

    @Test
    fun `create produces fresh independent stat`() {
        val r1 = Range(AtomicMode).apply { update(5.0) }
        val r2 = r1.create(SerialMode)
        r2.update(1.0)

        assertEquals(5.0, r1.read().min, DELTA)
        assertEquals(1.0, r2.read().min, DELTA)
    }

    @Test
    fun `read result carries name`() {
        val r = Range()
        r.update(10.0)
    }
}

class MinStatsTest {

    @Test
    fun `tracks minimum`() {
        val m = Min()
        m.update(3.0)
        m.update(1.0)
        m.update(5.0)
        assertEquals(1.0, m.read().min, DELTA)
    }

    @Test
    fun `empty returns positive infinity`() {
        assertEquals(Double.POSITIVE_INFINITY, Min().read().min)
    }

    @Test
    fun `merge takes smaller min`() {
        val m1 = Min().apply { update(4.0) }
        val m2 = Min().apply { update(2.0) }
        m1.merge(m2.read())
        assertEquals(2.0, m1.read().min, DELTA)
    }

    @Test
    fun `reset restores infinity`() {
        val m = Min().apply { update(3.0) }
        m.reset()
        assertEquals(Double.POSITIVE_INFINITY, m.read().min)
    }

    @Test
    fun `create produces fresh independent stat`() {
        val m1 = Min().apply { update(5.0) }
        val m2 = m1.create()
        m2.update(1.0)
        assertEquals(5.0, m1.read().min, DELTA)
        assertEquals(1.0, m2.read().min, DELTA)
    }
}

class MaxStatsTest {

    @Test
    fun `tracks maximum`() {
        val m = Max()
        m.update(3.0)
        m.update(7.0)
        m.update(2.0)
        assertEquals(7.0, m.read().max, DELTA)
    }

    @Test
    fun `empty returns negative infinity`() {
        assertEquals(Double.NEGATIVE_INFINITY, Max().read().max)
    }

    @Test
    fun `merge takes larger max`() {
        val m1 = Max().apply { update(4.0) }
        val m2 = Max().apply { update(9.0) }
        m1.merge(m2.read())
        assertEquals(9.0, m1.read().max, DELTA)
    }

    @Test
    fun `reset restores negative infinity`() {
        val m = Max().apply { update(7.0) }
        m.reset()
        assertEquals(Double.NEGATIVE_INFINITY, m.read().max)
    }

    @Test
    fun `create produces fresh independent stat`() {
        val m1 = Max().apply { update(5.0) }
        val m2 = m1.create()
        m2.update(10.0)
        assertEquals(5.0, m1.read().max, DELTA)
        assertEquals(10.0, m2.read().max, DELTA)
    }
}
