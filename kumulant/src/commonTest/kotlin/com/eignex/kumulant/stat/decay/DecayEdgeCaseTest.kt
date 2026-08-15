package com.eignex.kumulant.stat.decay

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Edge cases at the ends of the decay family's parameter and time ranges, where the arithmetic
 * stops being well conditioned: half-lives long enough to overflow the rotation threshold, short
 * enough to truncate to zero, timestamps that arrive out of order, and reads so late that the
 * shared decay factor has underflowed to zero.
 */
class DecayEdgeCaseTest {

    private val t0 = 1_000_000_000L

    @Test
    fun `a half-life too long to fit the rotation threshold still updates`() {
        // halfLife * 50 overflows Long above ~2135 days. It used to wrap negative, which made the
        // rotation test true for every dt and span update() forever.
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
        // inWholeNanoseconds truncates these to 0, which makes alpha infinite and every later
        // exp() NaN. Reject at construction rather than reporting NaN forever.
        assertFailsWith<IllegalArgumentException> { DecayWeighting.HalfLife(Duration.ZERO) }
        assertFailsWith<IllegalArgumentException> { DecayWeighting.HalfLife((-1).seconds) }
        DecayWeighting.HalfLife(1.nanoseconds) // the smallest that survives truncation
    }

    @Test
    fun `a negative or non-finite alpha is rejected but zero is allowed`() {
        assertFailsWith<IllegalArgumentException> { DecayWeighting.Alpha(-0.5) }
        assertFailsWith<IllegalArgumentException> { DecayWeighting.Alpha(Double.NaN) }
        assertFailsWith<IllegalArgumentException> { DecayWeighting.Alpha(Double.POSITIVE_INFINITY) }
        // Zero means "never move"; HoltStat uses it to disable trend smoothing.
        assertEquals(0.0, DecayWeighting.Alpha(0.0).correction(5.0))
    }

    @Test
    fun `the bias correction survives an alpha times weight too small for the closed form`() {
        // 1 - exp(-x) is exactly 0.0 for any x below the double epsilon, and callers divide by it.
        val weighting = DecayWeighting.Alpha(1e-300)

        val correction = weighting.correction(1.0)

        assertTrue(correction > 0.0, "correction underflowed to $correction")
        assertEquals(1e-300, correction, 1e-315)
    }

    @Test
    fun `a downdate keeps its negative correction`() {
        // The underflow fallback must not swallow the legitimately negative value here.
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

        // Both used to be reachable here; mean reported NaN and variance reported 0.0.
        assertEquals(0.0, mean.read(t0).mean)
        assertEquals(0.0, variance.read(t0).mean)
    }

    @Test
    fun `a backwards timestamp discounts the late sample instead of inflating the accumulator`() {
        val stat = DecayingVarianceStat(halfLife = 1.milliseconds())
        stat.update(1.0, t0 + 2_000_000_000L, 1.0)

        stat.update(2.0, t0, 1.0) // two seconds behind, i.e. thousands of half-lives late

        val r = stat.read(t0 + 2_000_000_000L)
        assertTrue(r.totalWeights.isFinite(), "totalWeights went non-finite: ${r.totalWeights}")
        assertTrue(r.mean.isFinite(), "mean went non-finite: ${r.mean}")
        // The late sample is discounted to nothing, so the state still reflects the first one.
        assertEquals(1.0, r.totalWeights, 1e-9)
        assertEquals(1.0, r.mean, 1e-9)

        // And the stat is not wedged: a later in-order update still lands.
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
        // The landmark starts at wall-clock construction time, so a stream numbering from its own
        // epoch is far behind it. That is not a late arrival and must not be discounted.
        val stat = DecayingVarianceStat(halfLife = 1.seconds)

        repeat(10) { stat.update(0.0, t0, 1.0) }

        assertEquals(10.0, stat.read(t0).totalWeights, 1e-9)
    }

    @Test
    fun `a decaying mean keeps reporting its ratio after the decay factor underflows`() {
        val stat = DecayingMeanStat(halfLife = 1.seconds)
        stat.update(100.0, t0, 1.0)

        // ~1100 half-lives on, the shared exp(-alpha*dt) is exactly 0.0 and flushes both sums.
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
