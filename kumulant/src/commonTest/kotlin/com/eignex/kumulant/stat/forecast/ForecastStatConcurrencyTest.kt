package com.eignex.kumulant.stat.forecast

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import kotlin.test.Test
import kotlin.test.assertEquals

/** Cross-mode smoke tests for the forecast family. */
class ForecastStatConcurrencyTest {

    private val values = doubleArrayOf(1.0, 2.5, -1.0, 3.0, 0.5, 4.0)
    private val timestamps =
        longArrayOf(0L, 1_000_000_000L, 2_000_000_000L, 3_000_000_000L, 4_000_000_000L, 5_000_000_000L)

    private fun <R : Result> sequentialReads(factory: (Concurrency) -> SeriesStat<R>): Map<Concurrency, R> =
        Concurrency.entries.associateWith { mode ->
            val s = factory(mode)
            for (i in values.indices) s.update(values[i], timestamps[i], 1.0)
            s.read(timestamps.last())
        }

    @Test
    fun `HoltStat sequential math equal across modes`() {
        val reads = sequentialReads { HoltStat(alpha = 0.3, beta = 0.1, phi = 0.9, concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.level, r.level, 1e-9, "Holt level mode=$mode")
            assertEquals(ref.trend, r.trend, 1e-9, "Holt trend mode=$mode")
        }
    }

    @Test
    fun `SeasonalSmoothingStat sequential math equal across modes`() {
        val reads = sequentialReads {
            SeasonalSmoothingStat(alpha = 0.3, beta = 0.1, gamma = 0.2, period = 3, concurrency = it)
        }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.level, r.level, 1e-9, "Seasonal level mode=$mode")
            assertEquals(ref.trend, r.trend, 1e-9, "Seasonal trend mode=$mode")
            assertEquals(ref.seasons, r.seasons, "Seasonal seasons mode=$mode")
            assertEquals(ref.currentSlot, r.currentSlot, "Seasonal slot mode=$mode")
        }
    }

    @Test
    fun `RecursiveVarianceStat sequential math equal across modes`() {
        val reads = sequentialReads {
            RecursiveVarianceStat(omega = 0.1, alpha = 0.2, beta = 0.7, concurrency = it)
        }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.variance, r.variance, 1e-9, "RecursiveVariance mode=$mode")
        }
    }
}
