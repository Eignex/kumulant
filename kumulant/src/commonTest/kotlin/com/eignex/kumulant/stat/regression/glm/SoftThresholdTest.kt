package com.eignex.kumulant.stat.regression.glm

import com.eignex.kumulant.DELTA
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SoftThresholdTest {

    @Test
    fun `shrinks toward zero by the threshold`() {
        assertEquals(0.7, softThreshold(1.0, 0.3), DELTA)
        assertEquals(-0.7, softThreshold(-1.0, 0.3), DELTA)
    }

    @Test
    fun `stops at zero rather than crossing it`() {
        assertEquals(0.0, softThreshold(0.2, 0.3), DELTA)
        assertEquals(0.0, softThreshold(-0.2, 0.3), DELTA)
        assertEquals(0.0, softThreshold(0.3, 0.3), DELTA)
    }

    @Test
    fun `a zero threshold is the identity`() {
        for (w in listOf(-2.5, -0.001, 0.0, 0.001, 2.5)) {
            assertEquals(w, softThreshold(w, 0.0), DELTA, "a zero threshold moved $w")
        }
    }

    @Test
    fun `a negative threshold leaves the coefficient alone instead of growing it`() {
        assertEquals(0.0, softThreshold(0.0, -0.5), DELTA)
        assertEquals(1.0, softThreshold(1.0, -0.5), DELTA)
        assertEquals(-1.0, softThreshold(-1.0, -0.5), DELTA)
    }

    @Test
    fun `is monotone and never increases magnitude`() {
        val threshold = 0.4
        var previous = Double.NEGATIVE_INFINITY
        var w = -3.0
        while (w <= 3.0) {
            val shrunk = softThreshold(w, threshold)
            assertTrue(shrunk >= previous - DELTA, "not monotone at w=$w: $previous then $shrunk")
            assertTrue(
                kotlin.math.abs(shrunk) <= kotlin.math.abs(w) + DELTA,
                "magnitude grew at w=$w: |$shrunk| > |$w|",
            )
            previous = shrunk
            w += 0.05
        }
    }

    @Test
    fun `the three hosts now agree on a negative lambda`() {
        val univariate = UnivariateRegressionStat(penalty = Penalty.L1(lambda = -1.0))
        val unpenalised = UnivariateRegressionStat()
        repeat(6) { i ->
            univariate.update(i.toDouble(), 2.0 * i)
            unpenalised.update(i.toDouble(), 2.0 * i)
        }
        assertEquals(
            unpenalised.read().slope,
            univariate.read().slope,
            1e-9,
            "a negative lambda still moved the univariate slope",
        )

        val diagonal = DiagonalRegressionStat(featureSize = 1, penalty = Penalty.L1(lambda = -1.0))
        val plain = DiagonalRegressionStat(featureSize = 1)
        repeat(6) { i ->
            diagonal.update(doubleArrayOf(i.toDouble()), 2.0 * i)
            plain.update(doubleArrayOf(i.toDouble()), 2.0 * i)
        }
        assertEquals(
            plain.read().weights[0],
            diagonal.read().weights[0],
            1e-9,
            "a negative lambda still moved the diagonal weight",
        )
    }

    @Test
    fun `a positive lambda still shrinks each host`() {
        val penalised = UnivariateRegressionStat(penalty = Penalty.L1(lambda = 0.5))
        val unpenalised = UnivariateRegressionStat()
        repeat(6) { i ->
            penalised.update(i.toDouble(), 2.0 * i)
            unpenalised.update(i.toDouble(), 2.0 * i)
        }
        val shrunk = penalised.read().slope
        val full = unpenalised.read().slope
        assertTrue(shrunk < full, "L1 did not shrink the univariate slope: $shrunk vs $full")
        assertTrue(shrunk >= 0.0, "L1 shrank past zero: $shrunk")
    }
}
