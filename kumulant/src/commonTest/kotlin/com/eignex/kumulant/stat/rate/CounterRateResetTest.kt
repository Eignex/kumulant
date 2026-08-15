package com.eignex.kumulant.stat.rate

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * A counter rate divides an accumulated total by a window, so the two have to describe the same
 * span. Both bugs here came from letting one move without the other.
 */
class CounterRateResetTest {

    private val second = 1_000_000_000L

    @Test
    fun `a counter reset re-anchors the window and the total together`() {
        val stat = CounterRateStat(treatDecreaseAsReset = true)
        stat.update(0.0, 0L)
        stat.update(100.0, 10 * second)

        stat.update(5.0, 10 * second + second / 2) // counter restarted

        // The window now starts at the restart, so the total must describe the post-restart span
        // only: 5 units over 0.5s. It used to keep all 105 pre-reset units and divide those by the
        // half-second, reporting 210/s.
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

        // The zero-weight call used to advance the high-water mark without accumulating, so the
        // increment was lost for good and the total came out 0.0 instead of 10.0.
        assertEquals(
            without.read(3 * second).totalValue,
            withZero.read(3 * second).totalValue,
            1e-9,
            "a zero-weight update must not destroy the increment",
        )
    }

    @Test
    fun `a rate with a large negative start timestamp does not silently report zero`() {
        // The Long subtraction used to overflow here and the duration guard read the wrapped value
        // as non-positive.
        val result = RateResult(startTimestampNanos = Long.MIN_VALUE + 1, totalValue = 100.0, timestampNanos = second)

        assertTrue(result.rate > 0.0, "overflowed duration reported rate ${result.rate}")
        assertTrue(result.rate.isFinite())
    }
}
