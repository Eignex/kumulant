package com.eignex.kumulant.stat.rate

import com.eignex.kumulant.DELTA
import kotlin.test.Test
import kotlin.test.assertEquals

private const val T0 = 1_000_000_000L
private const val T1 = 2_000_000_000L
private const val T2 = 3_000_000_000L

class RateStatTest {

    @Test
    fun `rate over one second with value 10`() {
        val r = RateStat()
        r.update(10.0, T0)
        val result = r.read(T1)

        assertEquals(10.0, result.rate, DELTA)
    }

    @Test
    fun `rate is zero when read at the same timestamp as the first update`() {
        val r = RateStat()
        r.update(10.0, T0)
        val result = r.read(T0)
        assertEquals(0.0, result.rate, 0.0)
    }

    @Test
    fun `rate accumulates across updates`() {
        val r = RateStat()
        r.update(3.0, T0)
        r.update(7.0, T0)
        val result = r.read(T1)
        assertEquals(10.0, result.rate, DELTA)
    }

    @Test
    fun `weighted update scales value`() {
        val r = RateStat()
        r.update(1.0, T0, weight = 5.0)
        val result = r.read(T1)
        assertEquals(5.0, result.rate, DELTA)
    }

    @Test
    fun `empty read before any update has zero total`() {
        val r = RateStat()
        assertEquals(0.0, r.read(T1).totalValue, DELTA)
    }

    @Test
    fun `merge takes earliest start and sums totals`() {
        val r1 = RateStat().apply { update(10.0, T0) }
        val r2 = RateStat().apply { update(5.0, T1) }
        r1.merge(r2.read())
        val result = r1.read(T2)
        assertEquals(15.0, result.totalValue, DELTA)

        assertEquals(T0, result.startTimestampNanos)
    }

    @Test
    fun `merge with zero total is no-op`() {
        val r1 = RateStat().apply { update(10.0, T0) }
        val before = r1.read(T1)
        r1.merge(RateResult(Long.MIN_VALUE, 0.0, T1))
        assertEquals(before.totalValue, r1.read(T1).totalValue, DELTA)
    }
}

class RateMergeWindowTest {

    @Test
    fun `merging a shard that observed only zeros keeps its window`() {
        val shardA = RateStat()
        for (t in 0..8) shardA.update(0.0, timestampNanos = t * 1_000_000_000L)
        val shardB = RateStat()
        shardB.update(100.0, timestampNanos = 9_000_000_000L)

        val merged = RateStat()
        merged.merge(shardA.read(10_000_000_000L))
        merged.merge(shardB.read(10_000_000_000L))

        val single = RateStat()
        for (t in 0..8) single.update(0.0, timestampNanos = t * 1_000_000_000L)
        single.update(100.0, timestampNanos = 9_000_000_000L)

        assertEquals(single.read(10_000_000_000L).rate, merged.read(10_000_000_000L).rate, DELTA)
    }

    @Test
    fun `merging an untouched shard does not stretch the window`() {
        val empty = RateStat()
        val merged = RateStat()
        merged.update(100.0, timestampNanos = 9_000_000_000L)
        merged.merge(empty.read(10_000_000_000L))
        assertEquals(9_000_000_000L, merged.read(10_000_000_000L).startTimestampNanos)
    }
}
