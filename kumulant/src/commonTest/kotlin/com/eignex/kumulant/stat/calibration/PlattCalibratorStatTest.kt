package com.eignex.kumulant.stat.calibration

import kotlin.math.exp
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private const val DELTA = 1e-9

class PlattCalibratorStatTest {

    @Test
    fun `calibrate is the sigmoid of slope times x plus intercept`() {
        val r = PlattCalibratorResult(slope = 2.0, intercept = -1.0, totalWeights = 100.0)
        val expected = 1.0 / (1.0 + exp(-(2.0 * 0.5 - 1.0)))
        assertEquals(expected, r.calibrate(0.5), DELTA)
    }

    @Test
    fun `learns a monotone mapping from raw scores to labels`() {
        // Underlying truth: label is more likely as raw score grows. Calibrator should
        // converge to a positive slope.
        val stat = PlattCalibratorStat()
        val rng = Random(7L)
        repeat(5000) {
            val raw = rng.nextDouble()
            val pTrue = raw
            val label = if (rng.nextDouble() < pTrue) 1.0 else 0.0
            stat.update(raw, label)
        }
        val r = stat.read()
        assertTrue(r.slope > 0.0, "slope=${r.slope} should be positive")
        // Calibrated probability at the top of the range should beat the bottom.
        assertTrue(r.calibrate(0.9) > r.calibrate(0.1))
    }
}
