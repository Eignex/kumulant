package com.eignex.kumulant.bandit

import com.eignex.kumulant.DELTA
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ExponentialWeightsTest {

    @Test
    fun `normalising shifts the largest log weight to zero and keeps every difference`() {
        val w = doubleArrayOf(800.0, 750.0, 900.0)

        w.normaliseLogWeights()

        assertEquals(-100.0, w[0], DELTA)
        assertEquals(-150.0, w[1], DELTA)
        assertEquals(0.0, w[2], DELTA)
    }

    @Test
    fun `normalising keeps a far-behind log weight finite`() {
        val w = doubleArrayOf(-Double.MAX_VALUE, Double.MAX_VALUE)

        w.normaliseLogWeights()

        assertTrue(w[0].isFinite(), "log weight left the finite range: ${w.toList()}")
        assertEquals(0.0, w[1], DELTA)
    }

    @Test
    fun `normalising is idempotent`() {
        val w = doubleArrayOf(800.0, 750.0, 900.0)

        w.normaliseLogWeights()
        val once = w.copyOf()
        w.normaliseLogWeights()

        for (i in w.indices) assertEquals(once[i], w[i], DELTA, "coordinate $i moved on the second pass")
    }

    @Test
    fun `an empty array is handled`() {
        DoubleArray(0).normaliseLogWeights()
    }

    @Test
    fun `sampleFromDistribution skips a zero-probability leading arm`() {
        val alwaysZero = object : Random() {
            override fun nextBits(bitCount: Int): Int = 0
        }
        assertEquals(1, alwaysZero.sampleFromDistribution(doubleArrayOf(0.0, 1.0)))
    }
}
