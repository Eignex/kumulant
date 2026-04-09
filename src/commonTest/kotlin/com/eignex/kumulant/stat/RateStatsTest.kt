package com.eignex.kumulant.stat

import com.eignex.kumulant.core.RateResult
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

private const val DELTA = 1e-9

// Fixed timestamps for deterministic tests (nanoseconds)
private const val T0 = 1_000_000_000L // 1 s
private const val T1 = 2_000_000_000L // 2 s
private const val T2 = 3_000_000_000L // 3 s
private const val T3 = 11_000_000_000L // 11 s

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
    fun `copy is independent`() {
        val r1 = Rate().apply { update(10.0, T0) }
        val r2 = r1.copy()
        r2.update(5.0, T1)
        assertEquals(10.0, r1.read(T1).totalValue, DELTA)
    }

    @Test
    fun `read result carries name`() {
        val r = Rate(name = "throughput")
        r.update(1.0, T0)
        assertEquals("throughput", r.read(T1).name)
    }
}

class DecayingRateTest {

    @Test
    fun `rate is positive after updates`() {
        val r = DecayingRate(halfLife = 1.seconds)
        r.update(1.0, T0)
        r.update(1.0, T1)
        assertTrue(r.read(T2).rate > 0.0)
    }

    @Test
    fun `rate decays toward zero far in the future`() {
        val r = DecayingRate(halfLife = 1.seconds)
        r.update(1.0, T0)
        val rateNear = r.read(T1).rate
        val rateFar = r.read(T3).rate // 10 s later — many half-lives
        assertTrue(rateFar < rateNear / 100.0, "rate should decay significantly over 10 half-lives")
    }

    @Test
    fun `reset yields zero rate`() {
        val r = DecayingRate(halfLife = 1.seconds)
        r.update(1.0, T0)
        r.reset()
        assertEquals(0.0, r.read(T1).rate, DELTA)
    }

    @Test
    fun `copy is independent`() {
        val r1 = DecayingRate(halfLife = 1.seconds)
        r1.update(10.0, T0)
        val r2 = r1.copy()
        r2.update(100.0, T1)
        // r1 must not see r2's update
        val rate1 = r1.read(T2).rate
        val rate2 = r2.read(T2).rate
        assertTrue(rate2 > rate1, "r2 should have higher rate after extra update")
    }
}
