package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.DELTA
import kotlin.test.Test
import kotlin.test.assertEquals

class PairedSumStatTest {

    @Test
    fun `sums weighted x and y independently`() {
        val stat = PairedSumStat().apply {
            update(x = 1.0, y = 2.0, timestampNanos = 0L, weight = 1.0)
            update(x = 3.0, y = 4.0, timestampNanos = 0L, weight = 2.0)
        }
        val r = stat.read(0L)
        assertEquals(3.0, r.totalWeights, DELTA)
        assertEquals(1.0 + 3.0 * 2.0, r.sumX, DELTA)
        assertEquals(2.0 + 4.0 * 2.0, r.sumY, DELTA)
    }

    @Test
    fun `zero weight is a noop`() {
        val stat = PairedSumStat().apply {
            update(99.0, 99.0, 0L, 0.0)
        }
        val r = stat.read(0L)
        assertEquals(0.0, r.totalWeights, DELTA)
        assertEquals(0.0, r.sumX, DELTA)
        assertEquals(0.0, r.sumY, DELTA)
    }

    @Test
    fun `merge adds component-wise`() {
        val a = PairedSumStat().apply { update(1.0, 1.0, 0L, 1.0) }
        val b = PairedSumStat().apply { update(2.0, 3.0, 0L, 1.0) }
        a.merge(b.read(0L))
        val r = a.read(0L)
        assertEquals(2.0, r.totalWeights, DELTA)
        assertEquals(3.0, r.sumX, DELTA)
        assertEquals(4.0, r.sumY, DELTA)
    }

    @Test
    fun `reset zeros all three accumulators`() {
        val stat = PairedSumStat().apply {
            update(1.0, 2.0, 0L, 1.0)
            reset()
        }
        val r = stat.read(0L)
        assertEquals(0.0, r.totalWeights, DELTA)
        assertEquals(0.0, r.sumX, DELTA)
        assertEquals(0.0, r.sumY, DELTA)
    }

    @Test
    fun `create produces an independent stat`() {
        val a = PairedSumStat().apply { update(1.0, 1.0, 0L, 1.0) }
        val b = a.create()
        b.update(5.0, 5.0, 0L, 1.0)
        assertEquals(1.0, a.read(0L).sumX, DELTA)
        assertEquals(5.0, b.read(0L).sumX, DELTA)
    }
}
