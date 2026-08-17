package com.eignex.kumulant.stat.calibration

import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class IsotonicCalibratorStatTest {

    @Test
    fun `calibration map is non-decreasing across bins`() {
        val stat = IsotonicCalibratorStat(numBins = 10)
        val rng = Random(11L)
        // Underlying truth: P(y = 1) grows with x; deliberately add a non-monotone
        // dip in the empirical rate by oversampling negatives near x = 0.6.
        repeat(2000) {
            val x = rng.nextDouble()
            val noisyTruth = if (x in 0.55..0.65) 0.2 else x
            val y = if (rng.nextDouble() < noisyTruth) 1.0 else 0.0
            stat.update(x, y)
        }
        val r = stat.read()
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
        // Two-bin case: half-positive in low bin, all-positive in high bin.
        val stat = IsotonicCalibratorStat(numBins = 2)
        repeat(10) { stat.update(0.25, if (it < 5) 1.0 else 0.0) } // bin 0: 0.5
        repeat(10) { stat.update(0.75, 1.0) } // bin 1: 1.0
        val r = stat.read()
        // Midpoints are 0.25 and 0.75; halfway between is 0.5 → interp probability 0.75.
        assertTrue(abs(r.calibrate(0.5) - 0.75) < 1e-9, "got=${r.calibrate(0.5)}")
        // Below the first midpoint clamps to the first bin's probability.
        assertTrue(abs(r.calibrate(0.1) - 0.5) < 1e-9)
        // Above the last midpoint clamps to the last bin's probability.
        assertTrue(abs(r.calibrate(0.9) - 1.0) < 1e-9)
    }

    @Test
    fun `merge is unsupported`() {
        val a = IsotonicCalibratorStat()
        val b = IsotonicCalibratorStat()
        a.update(0.5, 1.0)
        b.update(0.5, 0.0)
        assertFailsWith<UnsupportedOperationException> { a.merge(b.read()) }
    }
}
