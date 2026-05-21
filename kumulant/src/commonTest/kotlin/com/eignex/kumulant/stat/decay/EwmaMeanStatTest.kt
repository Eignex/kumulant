package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.stat.summary.WeightedMeanResult
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private const val DELTA = 1e-9

class EwmaMeanStatTest {
    private val delta = 1e-9

    @Test
    fun `EwmaMeanStat create produces fresh independent stat`() {
        val m1 = EwmaMeanStat(alpha = 0.5).apply { update(10.0) }
        val m2 = m1.create()
        repeat(10) { m1.update(10.0) }
        assertEquals(0.0, m2.read().mean, delta)
        assertTrue(m1.read().mean > 0.0)
    }

    @Test
    fun `EwmaMeanStat merge behavior`() {
        val d1 = EwmaMeanStat(alpha = 0.5)
        d1.update(10.0)
        val d2 = WeightedMeanResult(1.0, 20.0)

        d1.merge(d2)

        assertEquals(15.0, d1.read().mean, delta)
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
    fun `EwmaMeanStat reset clears state`() {
        val meanStat = EwmaMeanStat(alpha = 0.5)
        meanStat.update(10.0)
        meanStat.reset()
        assertEquals(0.0, meanStat.read().mean, delta)
    }

    @Test
    fun `EwmaMeanStat before any update returns zero total weight and zero mean`() {
        val m = EwmaMeanStat(alpha = 0.1)
        val r = m.read()
        assertEquals(0.0, r.totalWeights, DELTA)
        assertEquals(0.0, r.mean, DELTA)
    }
}
