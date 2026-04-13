package com.eignex.kumulant.stat

import com.eignex.kumulant.core.RateResult
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

private const val DELTA = 1e-9

// Fixed timestamps for deterministic tests (nanoseconds)
private const val T0 = 1_000_000_000L // 1 s
private const val T1 = 2_000_000_000L // 2 s
private const val T2 = 3_000_000_000L // 3 s

class RateTest {

    @Test
    fun `rate over one second with value 10`() {
        val r = Rate()
        r.update(10.0, T0)
        val result = r.read(T1)
        // 10 units over 1 second = 10 per second
        assertEquals(10.0, result.rate, DELTA)
    }

    @Test
    fun `rate accumulates across updates`() {
        val r = Rate()
        r.update(3.0, T0)
        r.update(7.0, T0)
        val result = r.read(T1)
        assertEquals(10.0, result.rate, DELTA)
    }

    @Test
    fun `rate over two seconds`() {
        val r = Rate()
        r.update(20.0, T0)
        val result = r.read(T2) // T2 - T0 = 2s
        assertEquals(10.0, result.rate, DELTA)
    }

    @Test
    fun `weighted update scales value`() {
        val r = Rate()
        r.update(1.0, T0, weight = 5.0)
        val result = r.read(T1)
        assertEquals(5.0, result.rate, DELTA)
    }

    @Test
    fun `empty read before any update has zero total`() {
        val r = Rate()
        assertEquals(0.0, r.read(T1).totalValue, DELTA)
    }

    @Test
    fun `merge takes earliest start and sums totals`() {
        val r1 = Rate().apply { update(10.0, T0) }
        val r2 = Rate().apply { update(5.0, T1) } // later start
        r1.merge(r2.read())
        val result = r1.read(T2)
        assertEquals(15.0, result.totalValue, DELTA)
        // start stays at T0
        assertEquals(T0, result.startTimestampNanos)
    }

    @Test
    fun `merge with zero total is no-op`() {
        val r1 = Rate().apply { update(10.0, T0) }
        val before = r1.read(T1)
        r1.merge(RateResult(Long.MIN_VALUE, 0.0, T1))
        assertEquals(before.totalValue, r1.read(T1).totalValue, DELTA)
    }

    @Test
    fun `reset clears start and total`() {
        val r = Rate().apply { update(10.0, T0) }
        r.reset()
        assertEquals(0.0, r.read(T1).totalValue, DELTA)
    }

    @Test
    fun `per extension rescales to per-minute`() {
        val r = Rate().apply { update(60.0, T0) }
        val result = r.read(T1) // 60 per second
        assertEquals(3600.0, result.per(60.seconds), DELTA)
    }

    @Test
    fun `create produces fresh independent stat`() {
        val r1 = Rate().apply { update(10.0, T0) }
        val r2 = r1.create()
        r2.update(5.0, T1)
        assertEquals(10.0, r1.read(T1).totalValue, DELTA)
    }
}

class CounterRateTest {

    @Test
    fun `derives rate from counter deltas`() {
        val r = CounterRate()
        r.update(100.0, T0)
        r.update(130.0, T1)
        val result = r.read(T1)
        assertEquals(30.0, result.totalValue, DELTA)
        assertEquals(30.0, result.rate, DELTA)
    }

    @Test
    fun `accumulates deltas across intervals`() {
        val r = CounterRate()
        r.update(100.0, T0)
        r.update(130.0, T1)
        r.update(160.0, T2)
        val result = r.read(T2)
        assertEquals(60.0, result.totalValue, DELTA)
        assertEquals(30.0, result.rate, DELTA)
    }

    @Test
    fun `single sample has zero derived total`() {
        val r = CounterRate()
        r.update(100.0, T0)
        assertEquals(0.0, r.read(T1).totalValue, DELTA)
    }

    @Test
    fun `counter decrease is treated as reset by default`() {
        val r = CounterRate()
        r.update(100.0, T0)
        r.update(10.0, T1) // reset happened between samples
        val result = r.read(T2)
        assertEquals(10.0, result.totalValue, DELTA)
        assertEquals(T1, result.startTimestampNanos)
        assertEquals(10.0, result.rate, DELTA)
    }

    @Test
    fun `counter decrease can be ignored`() {
        val r = CounterRate(treatDecreaseAsReset = false)
        r.update(100.0, T0)
        r.update(10.0, T1)
        assertEquals(0.0, r.read(T2).totalValue, DELTA)
    }

    @Test
    fun `out of order timestamp is ignored`() {
        val r = CounterRate()
        r.update(100.0, T1)
        r.update(120.0, T0) // ignored
        r.update(130.0, T2)
        val result = r.read(T2)
        assertEquals(30.0, result.totalValue, DELTA)
        assertEquals(30.0, result.rate, DELTA)
    }

    @Test
    fun `merge sums totals and keeps earliest start`() {
        val r1 = CounterRate().apply {
            update(100.0, T0)
            update(150.0, T1)
        }
        val r2 = CounterRate().apply {
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
        val r = CounterRate().apply {
            update(100.0, T0)
            update(130.0, T1)
        }
        r.reset()
        assertEquals(0.0, r.read(T2).totalValue, DELTA)
    }
}
