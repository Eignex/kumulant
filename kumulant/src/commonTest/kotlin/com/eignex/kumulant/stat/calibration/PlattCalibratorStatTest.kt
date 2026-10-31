package com.eignex.kumulant.stat.calibration

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.schema.optimizer.Sgd
import com.eignex.kumulant.stat.regression.glm.StepDecay
import kotlin.math.exp
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PlattCalibratorStatTest {

    @Test
    fun `calibrate is the sigmoid of slope times x plus intercept`() {
        val r = PlattCalibratorResult(slope = 2.0, intercept = -1.0, totalWeights = 100.0)
        val expected = 1.0 / (1.0 + exp(-(2.0 * 0.5 - 1.0)))
        assertEquals(expected, r.calibrate(0.5), DELTA)
    }

    @Test
    fun `learns a monotone mapping from raw scores to labels`() {
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
        assertTrue(r.calibrate(0.9) > r.calibrate(0.1))
    }

    @Test
    fun `a calibrator merged into a fresh one resumes the learning rate schedule`() {
        val optimizer = Sgd(StepDecay(eta = 1.0, k = 1.0))
        val trained = PlattCalibratorStat(optimizer)
        val rng = Random(1)
        repeat(2000) {
            val x = rng.nextDouble() * 4 - 2
            trained.update(x, if (rng.nextDouble() < 1 / (1 + exp(-2 * x))) 1.0 else 0.0)
        }
        val merged = PlattCalibratorStat(optimizer)
        merged.merge(trained.read())
        trained.update(2.0, 0.0)
        merged.update(2.0, 0.0)
        assertEquals(trained.read().slope, merged.read().slope, DELTA)
    }
}
