package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.stat.rate.DecayingRate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

private const val DELTA = 1e-12
private const val T0 = 1_000_000_000L
private const val T1 = 2_000_000_000L
private const val T2 = 3_000_000_000L
private const val T3 = 11_000_000_000L

class DerivedStatsTest {

    @Test
    fun `Count ignores incoming weights and counts updates`() {
        val count = Count()
        count.update(10.0, weight = 2.5)
        count.update(20.0, weight = 7.5)
        assertEquals(2.0, count.read().sum, DELTA)
    }

    @Test
    fun `TotalWeights sums incoming weights`() {
        val count = TotalWeights()
        count.update(10.0, weight = 2.5)
        count.update(20.0, weight = 7.5)
        assertEquals(10.0, count.read().sum, DELTA)
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
        val rateFar = r.read(T3).rate
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
    fun `create produces fresh independent stat`() {
        val r1 = DecayingRate(halfLife = 1.seconds)
        r1.update(10.0, T0)
        val r2 = r1.create()
        r2.update(100.0, T1)

        val rate1 = r1.read(T2).rate
        val rate2 = r2.read(T2).rate
        assertTrue(rate2 > rate1, "r2 should have higher rate after extra update")
    }
}
