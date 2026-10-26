package com.eignex.kumulant.math

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class NormalQuantileTest {
    @Test
    fun `quantiles agree with independent SciPy ndtri values through the tails`() {
        val cases = listOf(
            1e-16 to -8.222082216130435,
            1e-12 to -7.034483825301131,
            0.001 to -3.090232306167813,
            0.075 to -1.4395314709384563,
            0.1 to -1.2815515655446004,
            0.5 to 0.0,
            0.9 to 1.2815515655446004,
            0.925 to 1.4395314709384563,
            0.999 to 3.090232306167813,
            0.9999999999999999 to 8.209536151601387,
        )
        for ((probability, expected) in cases) assertEquals(expected, standardNormalQuantile(probability), 4e-15)
    }

    @Test
    fun `invalid probabilities are rejected`() {
        for (probability in listOf(0.0, 1.0, -0.1, 1.1, Double.NaN)) {
            assertFailsWith<IllegalArgumentException> { standardNormalQuantile(probability) }
        }
    }
}
