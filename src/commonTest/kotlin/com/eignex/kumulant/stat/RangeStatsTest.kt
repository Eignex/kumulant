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
        assertEquals(1.0, r.min, DELTA)
        assertEquals(5.0, r.max, DELTA)
    }

    @Test
    fun `single value makes min equal max`() {
        val r = Range()
        r.update(42.0)
        assertEquals(42.0, r.min, DELTA)
        assertEquals(42.0, r.max, DELTA)
    }

    @Test
    fun `negative values`() {
        val r = Range()
        r.update(-5.0)
        r.update(-10.0)
        r.update(-1.0)
        assertEquals(-10.0, r.min, DELTA)
        assertEquals(-1.0, r.max, DELTA)
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
        val r1 = Range().apply { update(2.0); update(8.0) }
        val r2 = Range().apply { update(1.0); update(5.0) }
        r1.merge(r2.read())
        assertEquals(1.0, r1.min, DELTA)
        assertEquals(8.0, r1.max, DELTA)
    }

    @Test
    fun `merge with empty other is no-op`() {
        val r1 = Range().apply { update(3.0); update(7.0) }
        val r2 = Range()  // empty: min=+inf, max=-inf
        r1.merge(r2.read())
        assertEquals(3.0, r1.min, DELTA)
        assertEquals(7.0, r1.max, DELTA)
    }

    @Test
    fun `reset clears state`() {
        val r = Range().apply { update(1.0); update(9.0) }
        r.reset()
        assertEquals(Double.POSITIVE_INFINITY, r.read().min)
        assertEquals(Double.NEGATIVE_INFINITY, r.read().max)
    }

    @Test
    fun `copy creates independent fresh stat`() {
        val r1 = Range(AtomicMode, "orig").apply { update(5.0) }
        val r2 = r1.copy(SerialMode, "copy")
        r2.update(1.0)
        // r1 unchanged by r2's update
        assertEquals(5.0, r1.min, DELTA)
        assertEquals(1.0, r2.min, DELTA)
        assertEquals("copy", r2.read().name)
    }

    @Test
    fun `read result carries name`() {
        val r = Range(name = "latency")
        r.update(10.0)
        assertEquals("latency", r.read().name)
    }
}
