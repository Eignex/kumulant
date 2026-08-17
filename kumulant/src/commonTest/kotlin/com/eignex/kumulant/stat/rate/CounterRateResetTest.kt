package com.eignex.kumulant.stat.rate

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// A counter rate divides an accumulated total by a window, so the two have to describe the same span.
class CounterRateResetTest {

    private val second = 1_000_000_000L

    @Test
    fun `a counter reset re-anchors the window and the total together`() {
        val stat = CounterRateStat(treatDecreaseAsReset = true)
        stat.update(0.0, 0L)
        stat.update(100.0, 10 * second)

        stat.update(5.0, 10 * second + second / 2) // counter restarted

        // The window starts at the restart, so the total must describe the post-restart span
        // only: 5 units over 0.5s.
        val rate = stat.read(11 * second).rate
        assertTrue(rate in 8.0..12.0, "expected roughly 10 events/s after the reset, got $rate")
    }

    @Test
    fun `a zero-weight update does not consume an increment`() {
        val withZero = CounterRateStat()
        withZero.update(0.0, 0L, 1.0)
        withZero.update(10.0, second, 0.0)
        withZero.update(10.0, 2 * second, 1.0)

        val without = CounterRateStat()
        without.update(0.0, 0L, 1.0)
        without.update(10.0, second, 1.0)
        without.update(10.0, 2 * second, 1.0)

        assertEquals(
            without.read(3 * second).totalValue,
            withZero.read(3 * second).totalValue,
            1e-9,
            "a zero-weight update must not destroy the increment",
        )
    }

    @Test
    fun `a rate with a large negative start timestamp does not silently report zero`() {
        // The Long subtraction overflows here, and a wrapped duration reads as non-positive.
        val result = RateResult(startTimestampNanos = Long.MIN_VALUE + 1, totalValue = 100.0, timestampNanos = second)

        assertTrue(result.rate > 0.0, "overflowed duration reported rate ${result.rate}")
        assertTrue(result.rate.isFinite())
    }
}
