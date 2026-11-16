package com.eignex.kumulant.stat.rate

import com.eignex.kumulant.core.Concurrency
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CounterRateResetTest {

    private val second = 1_000_000_000L

    @Test
    fun `reset and counter decreases replace accumulated totals in every mode`() {
        for (concurrency in Concurrency.entries) {
            val stat = CounterRateStat(concurrency)
            stat.update(0.0, 0L)
            stat.update(100.0, second)
            stat.update(5.0, 2 * second)
            stat.update(25.0, 3 * second)
            assertEquals(25.0, stat.read(3 * second).totalValue, "mode=$concurrency")
            assertEquals(2 * second, stat.read(3 * second).startTimestampNanos, "mode=$concurrency")
            stat.reset()
            stat.update(500.0, 4 * second)
            stat.update(510.0, 5 * second)
            assertEquals(10.0, stat.read(5 * second).totalValue, "mode=$concurrency")
        }
    }

    @Test
    fun `a counter reset re-anchors the window and the total together`() {
        val stat = CounterRateStat(treatDecreaseAsReset = true)
        stat.update(0.0, 0L)
        stat.update(100.0, 10 * second)

        stat.update(5.0, 10 * second + second / 2)

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
        val result = RateResult(startTimestampNanos = Long.MIN_VALUE + 1, totalValue = 100.0, timestampNanos = second)

        assertTrue(result.rate > 0.0, "overflowed duration reported rate ${result.rate}")
        assertTrue(result.rate.isFinite())
    }
}
