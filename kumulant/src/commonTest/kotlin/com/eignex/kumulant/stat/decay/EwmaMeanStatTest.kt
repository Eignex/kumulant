package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class EwmaMeanStatTest {

    @Test
    fun `EwmaMeanStat merge behavior`() {
        val d1 = EwmaMeanStat(alpha = 0.5)
        d1.update(10.0)
        val d2 = WeightedMeanResult(1.0, 20.0)

        d1.merge(d2)

        assertEquals(15.0, d1.read().mean, DELTA)
    }

    @Test
    fun `EwmaMeanStat biases toward heavy recent values`() {
        val stat = EwmaMeanStat(alpha = 0.5)
        stat.update(10.0, 1.0)
        stat.update(100.0, 10.0)

        assertTrue(
            stat.read().mean > 80.0,
            "MeanStat should heavily favor the massive recent update",
        )
    }

    @Test
    fun `EwmaMeanStat before any update returns zero total weight and zero mean`() {
        val m = EwmaMeanStat(alpha = 0.1)
        val r = m.read()
        assertEquals(0.0, r.totalWeights, DELTA)
        assertEquals(0.0, r.mean, DELTA)
    }
}
