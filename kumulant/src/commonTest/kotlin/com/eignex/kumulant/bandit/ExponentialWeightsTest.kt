package com.eignex.kumulant.bandit

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private const val DELTA = 1e-12

/**
 * The shared EXP3 / EXP4 weight renormaliser. Both policies had their own copy, spelled with different
 * branch structure, and the tests below are the ratio-preservation argument that makes the rescale safe.
 */
class ExponentialWeightsTest {

    @Test
    fun `weights inside the usable range are left alone`() {
        val w = doubleArrayOf(1.0, 2.0, 0.5)

        w.renormaliseExponentialWeights()

        assertEquals(1.0, w[0], DELTA)
        assertEquals(2.0, w[1], DELTA)
        assertEquals(0.5, w[2], DELTA)
    }

    @Test
    fun `an overflowing array is rescaled and keeps every ratio`() {
        val w = doubleArrayOf(1e200, 5e199, 2e200)
        val ratioBefore = w[0] / w[2]

        w.renormaliseExponentialWeights()

        assertTrue(w.all { it <= 1.0 }, "not brought back into range: ${w.toList()}")
        assertEquals(1.0, w[2], DELTA, "the maximum should land on exactly one")
        assertEquals(ratioBefore, w[0] / w[2], DELTA, "the rescale changed an arm's relative standing")
    }

    @Test
    fun `an underflowing array is rescaled rather than left to die`() {
        // The half that used to be unguarded in EXP3. A run of large negative rewards drives every
        // exp(eta * gain) toward zero; if the array is left there, one more step reaches exactly zero
        // and no later reward can lift it, because every update multiplies.
        val w = doubleArrayOf(1e-200, 5e-201, 2e-200)
        val ratioBefore = w[0] / w[2]

        w.renormaliseExponentialWeights()

        assertTrue(w.all { it >= 1e-100 }, "still in the underflow band: ${w.toList()}")
        assertEquals(1.0, w[2], DELTA, "the maximum should land on exactly one")
        assertEquals(ratioBefore, w[0] / w[2], DELTA, "the rescale changed an arm's relative standing")
    }

    @Test
    fun `a fully collapsed array resets to uniform`() {
        // Once every weight is exactly zero there are no ratios left to preserve, so there is nothing
        // to restore it to except a fresh uniform prior. Anything else divides by zero.
        val w = doubleArrayOf(0.0, 0.0, 0.0)

        w.renormaliseExponentialWeights()

        assertTrue(w.all { it == 1.0 }, "expected a uniform reset: ${w.toList()}")
    }

    @Test
    fun `a partly collapsed array survives on its remaining mass`() {
        // Distinct from the case above: one arm still carries weight, so the zeros stay zero and the
        // survivor is what the distribution rides on. Resetting to uniform here would throw away
        // everything the policy had learned.
        val w = doubleArrayOf(0.0, 1e-200, 0.0)

        w.renormaliseExponentialWeights()

        assertEquals(0.0, w[0], DELTA)
        assertEquals(1.0, w[1], DELTA)
        assertEquals(0.0, w[2], DELTA)
    }

    @Test
    fun `rescaling is idempotent`() {
        // A second pass must be a no-op, or the policies would drift every time they called it.
        val w = doubleArrayOf(1e200, 5e199, 2e200)

        w.renormaliseExponentialWeights()
        val once = w.copyOf()
        w.renormaliseExponentialWeights()

        for (i in w.indices) assertEquals(once[i], w[i], DELTA, "coordinate $i moved on the second pass")
    }

    @Test
    fun `an empty array is handled`() {
        DoubleArray(0).renormaliseExponentialWeights()
    }

    private fun DoubleArray.all(predicate: (Double) -> Boolean): Boolean {
        for (v in this) if (!predicate(v)) return false
        return true
    }
}
