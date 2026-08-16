package com.eignex.kumulant.stat.score

import com.eignex.kumulant.DELTA
import kotlin.test.Test
import kotlin.test.assertEquals

class BrierScoreTest {

    @Test
    fun `perfect forecasts score zero`() {
        val stat = BrierScoreStat().apply {
            update(x = 1.0, y = 1.0, timestampNanos = 0L, weight = 1.0)
            update(x = 0.0, y = 0.0, timestampNanos = 0L, weight = 1.0)
        }
        assertEquals(0.0, stat.read(0L).mean, DELTA)
    }

    @Test
    fun `worst-case wrong forecasts score one`() {
        val stat = BrierScoreStat().apply {
            update(x = 1.0, y = 0.0, timestampNanos = 0L, weight = 1.0)
            update(x = 0.0, y = 1.0, timestampNanos = 0L, weight = 1.0)
        }
        assertEquals(1.0, stat.read(0L).mean, DELTA)
    }

    @Test
    fun `mean of squared probability errors`() {
        val stat = BrierScoreStat().apply {
            update(0.7, 1.0, 0L, 1.0)
            update(0.4, 0.0, 0L, 1.0)
        }
        assertEquals(0.125, stat.read(0L).mean, DELTA)
    }
}
