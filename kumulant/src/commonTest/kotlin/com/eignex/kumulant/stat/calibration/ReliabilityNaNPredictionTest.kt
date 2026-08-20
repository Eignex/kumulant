package com.eignex.kumulant.stat.calibration

import com.eignex.kumulant.DELTA
import kotlin.test.Test
import kotlin.test.assertEquals

class ReliabilityNaNPredictionTest {

    @Test
    fun `a NaN prediction is not credited to the first bin`() {
        val withNaNs = IsotonicCalibratorStat(numBins = 4)
        repeat(100) { withNaNs.update(0.05, 0.0) }
        repeat(100) { withNaNs.update(0.9, 1.0) }
        repeat(100) { withNaNs.update(Double.NaN, 1.0) }

        val withoutNaNs = IsotonicCalibratorStat(numBins = 4)
        repeat(100) { withoutNaNs.update(0.05, 0.0) }
        repeat(100) { withoutNaNs.update(0.9, 1.0) }

        assertEquals(withoutNaNs.read().calibrate(0.05), withNaNs.read().calibrate(0.05), DELTA)
    }
}
