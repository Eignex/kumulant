package com.eignex.kumulant.stat.calibration

import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class IsotonicCalibratorResultTest {

    @Test
    fun `calibration map is non-decreasing across bins`() {
        val stat = ReliabilityStat(numBins = 10)
        val rng = Random(11L)
        repeat(2000) {
            val x = rng.nextDouble()
            val noisyTruth = if (x in 0.55..0.65) 0.2 else x
            val y = if (rng.nextDouble() < noisyTruth) 1.0 else 0.0
            stat.update(x, y)
        }
        val r = stat.read().isotonicCalibration()
        for (i in 1 until r.probabilities.size) {
            assertTrue(
                r.probabilities[i] >= r.probabilities[i - 1] - 1e-12,
                "non-monotone at $i: ${r.probabilities[i - 1]} -> ${r.probabilities[i]}",
            )
        }
        assertTrue(r.calibrate(0.05) < r.calibrate(0.95))
    }

    @Test
    fun `calibrate interpolates linearly between bin midpoints`() {
        val stat = ReliabilityStat(numBins = 2)
        repeat(10) { stat.update(0.25, if (it < 5) 1.0 else 0.0) }
        repeat(10) { stat.update(0.75, 1.0) }
        val r = stat.read().isotonicCalibration()
        assertTrue(abs(r.calibrate(0.5) - 0.75) < 1e-9, "got=${r.calibrate(0.5)}")
        assertTrue(abs(r.calibrate(0.1) - 0.5) < 1e-9)
        assertTrue(abs(r.calibrate(0.9) - 1.0) < 1e-9)
    }

    @Test
    fun `calibration pools bin evidence after merging reliability snapshots`() {
        val first = ReliabilityStat(numBins = 2)
        val second = ReliabilityStat(numBins = 2)
        first.update(0.25, 1.0, weight = 2.0)
        second.update(0.75, 0.0, weight = 6.0)

        first.merge(second.read())
        val calibration = first.read().isotonicCalibration()

        assertEquals(listOf(0.25, 0.25), calibration.probabilities.toList())
        assertEquals(8.0, calibration.totalWeights)
    }

    @Test
    fun `empty bins extend the nearest fitted pool`() {
        val stat = ReliabilityStat(numBins = 4)
        stat.update(0.375, 1.0, weight = 3.0)
        stat.update(0.375, 0.0)

        val calibration = stat.read().isotonicCalibration()

        assertEquals(List(4) { 0.75 }, calibration.probabilities.toList())
    }

    @Test
    fun `empty histogram calibrates to zero`() {
        val calibration = ReliabilityStat(numBins = 4).read().isotonicCalibration()

        assertEquals(List(4) { 0.0 }, calibration.probabilities.toList())
    }
}
