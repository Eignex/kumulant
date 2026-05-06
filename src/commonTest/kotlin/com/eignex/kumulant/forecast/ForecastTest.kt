package com.eignex.kumulant.forecast

import kotlin.math.PI
import kotlin.math.abs
import kotlin.math.exp
import kotlin.math.sqrt
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

private const val DELTA = 1e-6

class GaussianForecastTest {

    @Test
    fun `standard normal CRPS at the mean equals 2 phi - 1 over sqrt pi`() {
        val expected = 2.0 / sqrt(2.0 * PI) - 1.0 / sqrt(PI)
        assertEquals(expected, GaussianForecast(0.0, 1.0).crps(0.0), DELTA)
    }

    @Test
    fun `CRPS scales with stdDev`() {
        val a = GaussianForecast(0.0, 1.0).crps(2.0)
        val b = GaussianForecast(0.0, 2.0).crps(4.0)
        // Same z but doubled scale → CRPS doubles.
        assertEquals(2.0 * a, b, 1e-9)
    }

    @Test
    fun `point-mass forecast reduces to absolute error`() {
        assertEquals(3.0, GaussianForecast(7.0, 0.0).crps(4.0), 0.0)
        assertEquals(0.0, GaussianForecast(7.0, 0.0).crps(7.0), 0.0)
    }

    @Test
    fun `negative stdDev rejected`() {
        assertFailsWith<IllegalArgumentException> { GaussianForecast(0.0, -1.0) }
    }

    @Test
    fun `CRPS is non-negative`() {
        for (mu in -3..3) for (sd in 1..4) for (y in -5..5) {
            assertTrue(GaussianForecast(mu.toDouble(), sd.toDouble()).crps(y.toDouble()) >= -1e-12)
        }
    }
}

class EnsembleForecastTest {

    @Test
    fun `single-sample ensemble equals absolute error`() {
        assertEquals(3.0, EnsembleForecast(doubleArrayOf(7.0)).crps(4.0), 0.0)
    }

    @Test
    fun `hand-computed three-sample CRPS`() {
        // x = [1, 2, 3], y = 2
        //   meanAbs = (1 + 0 + 1) / 3 = 2/3
        //   pairwise: |1-2|+|1-3|+|2-1|+|2-3|+|3-1|+|3-2| = 8
        //   pairTerm = 8 / (2 * 9) = 4/9
        //   crps = 2/3 - 4/9 = 2/9
        val expected = 2.0 / 9.0
        assertEquals(expected, EnsembleForecast(doubleArrayOf(1.0, 2.0, 3.0)).crps(2.0), 1e-12)
    }

    @Test
    fun `unsorted input yields same score as sorted`() {
        val a = EnsembleForecast(doubleArrayOf(3.0, 1.0, 2.0)).crps(2.0)
        val b = EnsembleForecast(doubleArrayOf(1.0, 2.0, 3.0)).crps(2.0)
        assertEquals(a, b, 1e-12)
    }

    @Test
    fun `empty ensemble returns NaN`() {
        assertTrue(EnsembleForecast(doubleArrayOf()).crps(0.0).isNaN())
    }

    @Test
    fun `ensemble CRPS is non-negative for arbitrary ensembles`() {
        val rng = kotlin.random.Random(42)
        repeat(20) {
            val m = 1 + rng.nextInt(50)
            val xs = DoubleArray(m) { rng.nextDouble() * 10.0 - 5.0 }
            val y = rng.nextDouble() * 10.0 - 5.0
            assertTrue(EnsembleForecast(xs).crps(y) >= -1e-12)
        }
    }

    @Test
    fun `ensemble CRPS converges to Gaussian CRPS for large normal samples`() {
        val rng = kotlin.random.Random(7)
        val m = 5_000
        val mean = 1.5
        val sd = 0.7
        val samples = DoubleArray(m) {
            // Box-Muller, only used inside the test to generate reference samples.
            val u1 = rng.nextDouble().coerceAtLeast(1e-12)
            val u2 = rng.nextDouble()
            mean + sd * sqrt(-2.0 * kotlin.math.ln(u1)) * kotlin.math.cos(2.0 * PI * u2)
        }
        val y = 2.0
        val ensembleCrps = EnsembleForecast(samples).crps(y)
        val gaussianCrps = GaussianForecast(mean, sd).crps(y)
        // Loose tolerance — ensemble estimator has Monte-Carlo error.
        assertTrue(
            abs(ensembleCrps - gaussianCrps) < 0.05,
            "ensemble CRPS $ensembleCrps far from analytic $gaussianCrps"
        )
    }

    private fun ln(x: Double) = kotlin.math.ln(x)
    private fun cos(x: Double) = kotlin.math.cos(x)

    @Suppress("unused")
    private fun ensureUsed() { exp(0.0) }
}

class StdNormalTest {

    @Test
    fun `cdf at zero is one half`() {
        assertEquals(0.5, stdNormalCdf(0.0), 1e-7)
    }

    @Test
    fun `cdf at plus minus one`() {
        assertEquals(0.8413447, stdNormalCdf(1.0), 1e-6)
        assertEquals(0.1586553, stdNormalCdf(-1.0), 1e-6)
    }

    @Test
    fun `cdf at plus minus two`() {
        assertEquals(0.9772499, stdNormalCdf(2.0), 1e-6)
        assertEquals(0.0227501, stdNormalCdf(-2.0), 1e-6)
    }

    @Test
    fun `pdf at zero equals one over sqrt 2 pi`() {
        assertEquals(1.0 / sqrt(2.0 * PI), stdNormalPdf(0.0), 1e-12)
    }

    @Test
    fun `pdf is symmetric`() {
        for (z in 1..3) {
            assertEquals(stdNormalPdf(z.toDouble()), stdNormalPdf(-z.toDouble()), 1e-12)
        }
    }
}
