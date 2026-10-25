package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.DELTA
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RangeStatTest {

    @Test
    fun `tracks min and max`() {
        val r = RangeStat()
        r.update(3.0)
        r.update(1.0)
        r.update(5.0)
        r.update(2.0)
        assertEquals(1.0, r.read().min, DELTA)
        assertEquals(5.0, r.read().max, DELTA)
    }

    @Test
    fun `single value makes min equal max`() {
        val r = RangeStat()
        r.update(42.0)
        assertEquals(42.0, r.read().min, DELTA)
        assertEquals(42.0, r.read().max, DELTA)
    }

    @Test
    fun `negative values`() {
        val r = RangeStat()
        r.update(-5.0)
        r.update(-10.0)
        r.update(-1.0)
        assertEquals(-10.0, r.read().min, DELTA)
        assertEquals(-1.0, r.read().max, DELTA)
    }

    @Test
    fun `empty stat returns infinities`() {
        val r = RangeStat()
        val result = r.read()
        assertEquals(Double.POSITIVE_INFINITY, result.min)
        assertEquals(Double.NEGATIVE_INFINITY, result.max)
    }

    @Test
    fun `merge combines ranges`() {
        val r1 = RangeStat().apply {
            update(2.0)
            update(8.0)
        }
        val r2 = RangeStat().apply {
            update(1.0)
            update(5.0)
        }
        r1.merge(r2.read())
        assertEquals(1.0, r1.read().min, DELTA)
        assertEquals(8.0, r1.read().max, DELTA)
    }

    @Test
    fun `merge with empty other is no-op`() {
        val r1 = RangeStat().apply {
            update(3.0)
            update(7.0)
        }
        val r2 = RangeStat()
        r1.merge(r2.read())
        assertEquals(3.0, r1.read().min, DELTA)
        assertEquals(7.0, r1.read().max, DELTA)
    }

    @Test
    fun `positive infinity sets max`() {
        val r = RangeStat()
        r.update(1.0)
        r.update(Double.POSITIVE_INFINITY)
        val result = r.read()
        assertEquals(1.0, result.min, DELTA)
        assertTrue(result.max.isInfinite() && result.max > 0.0)
    }

    @Test
    fun `negative infinity sets min`() {
        val r = RangeStat()
        r.update(1.0)
        r.update(Double.NEGATIVE_INFINITY)
        val result = r.read()
        assertTrue(result.min.isInfinite() && result.min < 0.0)
        assertEquals(1.0, result.max, DELTA)
    }

    @Test
    fun `NaN is ignored by the less-than and greater-than comparisons`() {
        val r = RangeStat()
        r.update(5.0)
        r.update(Double.NaN)
        val result = r.read()
        assertEquals(5.0, result.min, DELTA)
        assertEquals(5.0, result.max, DELTA)
    }
}
