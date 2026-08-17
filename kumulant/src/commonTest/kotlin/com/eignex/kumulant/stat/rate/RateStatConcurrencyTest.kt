package com.eignex.kumulant.stat.rate

import com.eignex.kumulant.assertModesAgree
import com.eignex.kumulant.core.Concurrency
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

class RateStatConcurrencyTest {

    private val timestamps = longArrayOf(0L, 1_000_000_000L, 2_000_000_000L, 3_500_000_000L, 5_000_000_000L)
    private val values = doubleArrayOf(1.0, 2.0, 1.0, 3.0, 1.0)

    @Test
    fun `RateStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = RateStat(concurrency = mode)
            for (i in values.indices) s.update(values[i], timestamps[i])
            s.read(timestamps.last())
        }
        assertModesAgree("RateStat", reads)
    }

    @Test
    fun `CounterRateStat sequential math equal across modes`() {
        val counters = doubleArrayOf(10.0, 20.0, 35.0, 50.0, 70.0)
        val reads = Concurrency.entries.associateWith { mode ->
            val s = CounterRateStat(concurrency = mode)
            for (i in counters.indices) s.update(counters[i], timestamps[i])
            s.read(timestamps.last())
        }
        assertModesAgree("CounterRateStat", reads)
    }

    @Test
    fun `DecayingRateStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = DecayingRateStat(halfLife = 2.seconds, concurrency = mode)
            for (i in values.indices) s.update(values[i], timestamps[i])
            s.read(timestamps.last())
        }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            // Different add-orderings can produce ULP-level differences; use a tight tolerance.
            assertEquals(ref.rate, r.rate, 1e-12, "DecayingRate rate mode=$mode")
            assertEquals(ref.timestampNanos, r.timestampNanos, "DecayingRate timestampNanos mode=$mode")
        }
    }
}
