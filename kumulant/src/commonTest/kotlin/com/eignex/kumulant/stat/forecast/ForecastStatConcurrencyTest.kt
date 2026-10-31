package com.eignex.kumulant.stat.forecast

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.timedReads
import kotlin.test.Test
import kotlin.test.assertEquals

class ForecastStatConcurrencyTest {

    @Test
    fun `ExponentialSmoothingStat sequential math equal across modes`() {
        val reads = timedReads {
            ExponentialSmoothingStat(alpha = 0.3, beta = 0.1, gamma = 0.2, period = 3, concurrency = it)
        }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.level, r.level, 1e-9, "Seasonal level mode=$mode")
            assertEquals(ref.trend, r.trend, 1e-9, "Seasonal trend mode=$mode")
            assertEquals(ref.seasons, r.seasons, "Seasonal seasons mode=$mode")
            assertEquals(ref.currentSlot, r.currentSlot, "Seasonal slot mode=$mode")
        }
    }

}
