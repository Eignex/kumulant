package com.eignex.kumulant.stat.rate

import com.eignex.kumulant.DELTA
import kotlin.test.Test
import kotlin.test.assertEquals

private const val T0 = 1_000_000_000L
private const val T1 = 2_000_000_000L
private const val T2 = 3_000_000_000L

class CounterRateStatTest {

    @Test
    fun `derives rate from counter deltas`() {
        val r = CounterRateStat()
        r.update(100.0, T0)
        r.update(130.0, T1)
        val result = r.read(T1)
        assertEquals(30.0, result.totalValue, DELTA)
        assertEquals(30.0, result.rate, DELTA)
    }

    @Test
    fun `accumulates deltas across intervals`() {
        val r = CounterRateStat()
        r.update(100.0, T0)
        r.update(130.0, T1)
        r.update(160.0, T2)
        val result = r.read(T2)
        assertEquals(60.0, result.totalValue, DELTA)
        assertEquals(30.0, result.rate, DELTA)
    }

    @Test
    fun `single sample has zero derived total`() {
        val r = CounterRateStat()
        r.update(100.0, T0)
        assertEquals(0.0, r.read(T1).totalValue, DELTA)
    }

    @Test
    fun `counter decrease is treated as reset by default`() {
        val r = CounterRateStat()
        r.update(100.0, T0)
        r.update(10.0, T1)
        val result = r.read(T2)
        assertEquals(10.0, result.totalValue, DELTA)
        // The reset re-anchors the start window to the post-reset timestamp;
        // the rate covers only post-reset progress.
        assertEquals(T1, result.startTimestampNanos)
        assertEquals(10.0, result.rate, DELTA)
    }

    @Test
    fun `counter decrease can be ignored`() {
        val r = CounterRateStat(treatDecreaseAsReset = false)
        r.update(100.0, T0)
        r.update(10.0, T1)
        assertEquals(0.0, r.read(T2).totalValue, DELTA)
    }

    @Test
    fun `out of order timestamp still contributes its forward delta`() {
        val r = CounterRateStat()
        r.update(100.0, T1)
        // The second sample carries a smaller timestamp than the first but a
        // larger counter value; a routine occurrence under concurrent
        // writers. Ordering is by counter value, not timestamp, so this
        // contributes its forward delta of 20 and lowers the start window.
        r.update(120.0, T0)
        r.update(130.0, T2)
        val result = r.read(T2)
        assertEquals(30.0, result.totalValue, DELTA)
        assertEquals(T0, result.startTimestampNanos)
    }

    @Test
    fun `merge sums totals and keeps earliest start`() {
        val r1 = CounterRateStat().apply {
            update(100.0, T0)
            update(150.0, T1)
        }
        val r2 = CounterRateStat().apply {
            update(10.0, T1)
            update(40.0, T2)
        }
        r1.merge(r2.read(T2))
        val result = r1.read(T2)
        assertEquals(80.0, result.totalValue, DELTA)
        assertEquals(T0, result.startTimestampNanos)
        assertEquals(40.0, result.rate, DELTA)
    }

    @Test
    fun `reset clears derived state`() {
        val r = CounterRateStat().apply {
            update(100.0, T0)
            update(130.0, T1)
        }
        r.reset()
        assertEquals(0.0, r.read(T2).totalValue, DELTA)
    }
}

class CounterRateZeroResetTest {

    @Test
    fun `a counter restarting at zero re-anchors the window`() {
        val s = CounterRateStat()
        s.update(0.0, timestampNanos = 0L)
        s.update(100.0, timestampNanos = 10_000_000_000L)
        s.update(0.0, timestampNanos = 20_000_000_000L)
        s.update(30.0, timestampNanos = 30_000_000_000L)

        val r = s.read(30_000_000_000L)
        assertEquals(30.0, r.totalValue, DELTA)
        assertEquals(20_000_000_000L, r.startTimestampNanos)
    }

    @Test
    fun `a negative weight leaves the counter increment recoverable`() {
        val s = CounterRateStat()
        s.update(0.0, timestampNanos = 0L, weight = 1.0)
        s.update(100.0, timestampNanos = 1_000_000_000L, weight = -1.0)
        s.update(200.0, timestampNanos = 2_000_000_000L, weight = 1.0)
        assertEquals(200.0, s.read(2_000_000_000L).totalValue, DELTA)
    }
}
