package com.eignex.kumulant.stat.rate

import com.eignex.kumulant.DELTA
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

private const val T0 = 1_000_000_000L
private const val T1 = 2_000_000_000L
private const val T2 = 3_000_000_000L
private const val T3 = 11_000_000_000L

class DecayingRateStatTest {

    @Test
    fun `rate is positive after updates`() {
        val r = DecayingRateStat(halfLife = 1.seconds)
        r.update(1.0, T0)
        r.update(1.0, T1)
        assertTrue(r.read(T2).rate > 0.0)
    }

    @Test
    fun `rate decays toward zero far in the future`() {
        val r = DecayingRateStat(halfLife = 1.seconds)
        r.update(1.0, T0)
        val rateNear = r.read(T1).rate
        val rateFar = r.read(T3).rate
        assertTrue(rateFar < rateNear / 100.0, "rate should decay significantly over 10 half-lives")
    }

    @Test
    fun `merge preserves negative rates`() {
        // DecayingSumStat (the underlying primitive) accepts negative values; the rate
        // projection must round-trip them through merge instead of clamping to zero.
        val source = DecayingRateStat(halfLife = 1.seconds)
        source.update(-1.0, T0)
        val negative = source.read(T0)
        assertTrue(negative.rate < 0.0)

        val target = DecayingRateStat(halfLife = 1.seconds)
        target.merge(negative)
        assertEquals(negative.rate, target.read(T0).rate, DELTA)
    }
}
