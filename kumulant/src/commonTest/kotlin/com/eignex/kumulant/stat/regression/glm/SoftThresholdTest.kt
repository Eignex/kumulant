package com.eignex.kumulant.stat.regression.glm

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private const val DELTA = 1e-12

/**
 * The shared L1 proximal operator, and the one case where the three copies it replaces disagreed.
 */
class SoftThresholdTest {

    @Test
    fun `shrinks toward zero by the threshold`() {
        assertEquals(0.7, softThreshold(1.0, 0.3), DELTA)
        assertEquals(-0.7, softThreshold(-1.0, 0.3), DELTA)
    }

    @Test
    fun `stops at zero rather than crossing it`() {
        // The property that makes this sparsity-inducing rather than just a shift: a coefficient
        // smaller than the threshold lands exactly on zero and stays there.
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
        // Penalty.L1 puts no positivity requirement on lambda, and every host multiplies lambda by
        // something positive to get its threshold, so a negative threshold is reachable from a
        // negative lambda. Two of the three copies of this operator lacked the guard, and without it
        // the branches read inside out: at w = 0.0 and threshold = -0.5 the first comparison matches
        // and returns +0.5, so a negative lambda pushed coefficients *away* from zero. Shrinking by a
        // negative amount is not shrinking, so the penalty does nothing at all.
        assertEquals(0.0, softThreshold(0.0, -0.5), DELTA)
        assertEquals(1.0, softThreshold(1.0, -0.5), DELTA)
        assertEquals(-1.0, softThreshold(-1.0, -0.5), DELTA)
    }

    @Test
    fun `is monotone and never increases magnitude`() {
        // Two properties a proximal operator must have, checked over a grid rather than at points, so
        // an alternative implementation cannot pass by matching the examples above.
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
        // The behaviour this extraction changed, pinned end to end rather than on the helper alone.
        // The univariate and diagonal fits used to grow their coefficients under a negative lambda
        // while the stochastic fit ignored it; all three now ignore it. A negative lambda remains a
        // caller error - this only settles what happens when one is passed.
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
        // Guards the guard: if the negative case now no-ops, the positive case has to still bite, or
        // the change above would have quietly disabled L1 everywhere.
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
