package com.eignex.kumulant.stat.forecast

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.timedReads
import kotlin.test.Test
import kotlin.test.assertEquals

/** Cross-mode smoke tests for the forecast family. */
class ForecastStatConcurrencyTest {

    @Test
    fun `HoltStat sequential math equal across modes`() {
        val reads = timedReads { HoltStat(alpha = 0.3, beta = 0.1, phi = 0.9, concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.level, r.level, 1e-9, "Holt level mode=$mode")
            assertEquals(ref.trend, r.trend, 1e-9, "Holt trend mode=$mode")
        }
    }

    @Test
    fun `SeasonalSmoothingStat sequential math equal across modes`() {
        val reads = timedReads {
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
        val reads = timedReads {
            RecursiveVarianceStat(omega = 0.1, alpha = 0.2, beta = 0.7, concurrency = it)
        }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.variance, r.variance, 1e-9, "RecursiveVariance mode=$mode")
        }
    }
}
