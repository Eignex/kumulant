package com.eignex.kumulant.stat.decay

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds

class DecayEdgeCaseTest {

    private val t0 = 1_000_000_000L

    @Test
    fun `a half-life too long to fit the rotation threshold still updates`() {
        for (halfLife in listOf(2136.days, 3650.days, Duration.INFINITE)) {
            val stat = DecayingSumStat(halfLife = halfLife)

            stat.update(1.0, t0, 1.0)
            stat.update(2.0, t0 + 1_000_000_000L, 1.0)

            val sum = stat.read(t0 + 2_000_000_000L).sum
            assertTrue(sum.isFinite(), "half-life $halfLife produced a non-finite sum: $sum")
            assertEquals(3.0, sum, 1e-6, "half-life $halfLife should barely decay over two seconds")
        }
    }

    @Test
    fun `a half-life under one nanosecond is rejected`() {
        assertFailsWith<IllegalArgumentException> { DecayWeighting.HalfLife(Duration.ZERO) }
        assertFailsWith<IllegalArgumentException> { DecayWeighting.HalfLife((-1).seconds) }
        DecayWeighting.HalfLife(1.nanoseconds)
    }

    @Test
    fun `a negative or non-finite alpha is rejected but zero is allowed`() {
        assertFailsWith<IllegalArgumentException> { DecayWeighting.Alpha(-0.5) }
        assertFailsWith<IllegalArgumentException> { DecayWeighting.Alpha(Double.NaN) }
        assertFailsWith<IllegalArgumentException> { DecayWeighting.Alpha(Double.POSITIVE_INFINITY) }
        assertEquals(0.0, DecayWeighting.Alpha(0.0).correction(5.0))
    }

    @Test
    fun `the bias correction survives an alpha times weight too small for the closed form`() {
        val weighting = DecayWeighting.Alpha(1e-300)

        val correction = weighting.correction(1.0)

        assertTrue(correction > 0.0, "correction underflowed to $correction")
        assertEquals(1e-300, correction, 1e-315)
    }

    @Test
    fun `a downdate keeps its negative correction`() {
        val weighting = DecayWeighting.Alpha(0.5)

        assertTrue(weighting.correction(-1.0) < 0.0)
    }

    @Test
    fun `EwmaMean and EwmaVariance agree when the correction cannot move`() {
        val mean = EwmaMeanStat(alpha = 0.0)
        val variance = EwmaVarianceStat(alpha = 0.0)
        repeat(2) {
            mean.update(10.0, t0, 1.0)
            variance.update(10.0, t0, 1.0)
        }

        assertEquals(0.0, mean.read(t0).mean)
        assertEquals(0.0, variance.read(t0).mean)
    }

    @Test
    fun `a backwards timestamp discounts the late sample instead of inflating the accumulator`() {
        val stat = DecayingVarianceStat(halfLife = 1.milliseconds())
        stat.update(1.0, t0 + 2_000_000_000L, 1.0)

        stat.update(2.0, t0, 1.0)

        val r = stat.read(t0 + 2_000_000_000L)
        assertTrue(r.totalWeights.isFinite(), "totalWeights went non-finite: ${r.totalWeights}")
        assertTrue(r.mean.isFinite(), "mean went non-finite: ${r.mean}")
        assertEquals(1.0, r.totalWeights, 1e-9)
        assertEquals(1.0, r.mean, 1e-9)

        stat.update(3.0, t0 + 3_000_000_000L, 1.0)
        assertTrue(stat.read(t0 + 3_000_000_000L).mean.isFinite())
    }

    @Test
    fun `a read behind the landmark does not inflate the reported weight`() {
        val stat = DecayingVarianceStat(halfLife = 1.seconds)
        stat.update(5.0, t0 + 10_000_000_000L, 1.0)

        val behind = stat.read(t0)

        assertEquals(1.0, behind.totalWeights, 1e-9, "a backwards read must not inflate the weight")
    }

    @Test
    fun `an empty variance stat still snaps its landmark to a replay clock`() {
        val stat = DecayingVarianceStat(halfLife = 1.seconds)

        repeat(10) { stat.update(0.0, t0, 1.0) }

        assertEquals(10.0, stat.read(t0).totalWeights, 1e-9)
    }

    @Test
    fun `a decaying mean keeps reporting its ratio after the decay factor underflows`() {
        val stat = DecayingMeanStat(halfLife = 1.seconds)
        stat.update(100.0, t0, 1.0)

        val late = stat.read(t0 + 1_100_000_000_000L)

        assertEquals(0.0, late.totalWeights, "the evidence has decayed away and should say so")
        assertEquals(100.0, late.mean, 1e-9, "the ratio is still recoverable and beats reporting 0.0")
    }

    @Test
    fun `an empty decaying mean still reports zero`() {
        val stat = DecayingMeanStat(halfLife = 1.seconds)

        assertEquals(0.0, stat.read(t0).mean)
        assertEquals(0.0, stat.read(t0).totalWeights)
    }

    private fun Int.milliseconds() = (this * 1_000_000L).nanoseconds
}
