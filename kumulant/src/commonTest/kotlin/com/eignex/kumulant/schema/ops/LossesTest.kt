package com.eignex.kumulant.schema.ops

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.schema.runtime.materialize
import kotlin.math.ln
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class LossesTest {

    @Test
    fun `mseLoss on binary probability errors yields the Brier score`() {
        for ((prediction, outcome, expected) in listOf(
            Triple(1.0, 1.0, 0.0),
            Triple(0.0, 0.0, 0.0),
            Triple(1.0, 0.0, 1.0),
            Triple(0.0, 1.0, 1.0),
            Triple(0.7, 1.0, 0.09),
        )) {
            val stat = mseLoss().materialize()

            stat.update(prediction, outcome)

            assertEquals(expected, stat.read().mean, DELTA)
        }
    }

    @Test
    fun `mseLoss weights each squared residual by its update weight`() {
        val stat = mseLoss().materialize()

        stat.update(2.0, 0.0, 0L, 2.0)
        stat.update(0.0, 0.0, 0L, 1.0)

        assertEquals(8.0 / 3.0, stat.read(0L).mean, DELTA)
        assertEquals(3.0, stat.read(0L).totalWeights, DELTA)
    }

    @Test
    fun `maeLoss is the mean of absolute residuals`() {
        val stat = maeLoss().materialize()

        stat.update(2.0, 1.0, 0L, 1.0)
        stat.update(-1.0, 1.0, 0L, 1.0)
        stat.update(0.0, 0.0, 0L, 1.0)

        assertEquals(1.0, stat.read(0L).mean, DELTA)
    }

    @Test
    fun `logLoss of a confident correct prediction is minus ln of its probability`() {
        val stat = logLoss().materialize()

        stat.update(0.9, 1.0, 0L, 1.0)
        stat.update(0.1, 0.0, 0L, 1.0)

        assertEquals(-ln(0.9), stat.read(0L).mean, DELTA)
    }

    @Test
    fun `logLoss clamps a confident wrong prediction to a finite loss`() {
        val stat = logLoss().materialize()

        stat.update(0.0, 1.0, 0L, 1.0)

        val mean = stat.read(0L).mean
        assertTrue(mean.isFinite())
        assertEquals(-ln(1e-15), mean, 1e-9)
    }

    @Test
    fun `pinballLoss at one half is half the absolute error`() {
        val stat = pinballLoss(0.5).materialize()

        stat.update(1.0, 3.0, 0L, 1.0)
        stat.update(4.0, 1.0, 0L, 1.0)

        assertEquals(1.25, stat.read(0L).mean, DELTA)
    }

    @Test
    fun `pinballLoss at a high tau penalizes under prediction more`() {
        val tau = 0.9
        val stat = pinballLoss(tau).materialize()

        stat.update(0.0, 1.0, 0L, 1.0)
        stat.update(1.0, 0.0, 0L, 1.0)

        assertEquals((tau * 1.0 + (1.0 - tau) * 1.0) / 2.0, stat.read(0L).mean, DELTA)
    }

    @Test
    fun `pinballLoss rejects a tau outside the unit interval`() {
        assertFailsWith<IllegalArgumentException> { pinballLoss(-0.1) }
        assertFailsWith<IllegalArgumentException> { pinballLoss(1.1) }
    }
}
