package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.math.DenseMatrix
import com.eignex.kumulant.math.DenseVector
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Custom prior + empirical-Bayes population fitting for [BayesianLinearRegression].
 * The custom prior is what lets klause persist a posterior across solver calls -
 * the previous read() snapshot is fed back in as the next instance's prior.
 */
class BayesianLinearRegressionPriorTest {

    @Test
    fun `default prior matches isotropic Gaussian with priorVariance on the diagonal`() {
        val blr = BayesianLinearRegression(featureSize = 3, priorVariance = 2.0)
        val r = blr.read()
        for (i in 0 until 3) {
            assertEquals(0.0, r.weights[i], 1e-12)
            assertEquals(2.0, r.covariance[i, i], 1e-12)
            for (j in 0 until 3) if (i != j) assertEquals(0.0, r.covariance[i, j], 1e-12)
        }
    }

    @Test
    fun `custom prior seeds the initial weights and covariance`() {
        val mean = DenseVector.of(doubleArrayOf(0.5, -1.0, 2.0))
        val cov = DenseMatrix.of(
            arrayOf(
                doubleArrayOf(1.0, 0.3, 0.0),
                doubleArrayOf(0.3, 1.0, 0.0),
                doubleArrayOf(0.0, 0.0, 0.5),
            )
        )
        val blr = BayesianLinearRegression(
            featureSize = 3,
            priorMean = mean,
            priorCovariance = cov,
        )
        val r = blr.read()
        for (i in 0 until 3) {
            assertEquals(mean[i], r.weights[i], 1e-12)
            for (j in 0 until 3) assertEquals(cov[i, j], r.covariance[i, j], 1e-12)
        }
    }

    @Test
    fun `non positive definite prior covariance is rejected at construction`() {
        // Diagonal with a negative entry - immediately non-PD.
        val bad = DenseMatrix.of(
            arrayOf(
                doubleArrayOf(1.0, 0.0),
                doubleArrayOf(0.0, -0.1),
            )
        )
        assertFailsWith<IllegalArgumentException> {
            BayesianLinearRegression(featureSize = 2, priorCovariance = bad)
        }
    }

    @Test
    fun `reset restores configured prior`() {
        val mean = DenseVector.of(doubleArrayOf(1.0, -0.5))
        val cov = DenseMatrix.diagonal(2, 0.25)
        val blr = BayesianLinearRegression(featureSize = 2, priorMean = mean, priorCovariance = cov)
        val rng = Random(7)
        repeat(200) {
            val x = DoubleArray(2) { rng.nextDouble() * 2 - 1 }
            blr.update(x, 3.0 * x[0] - x[1], 1.0)
        }
        blr.reset()
        val r = blr.read()
        for (i in 0 until 2) {
            assertEquals(mean[i], r.weights[i], 1e-12)
            assertEquals(cov[i, i], r.covariance[i, i], 1e-12)
        }
        assertEquals(0L, r.step)
        assertEquals(0.0, r.totalWeights, 1e-12)
        assertEquals(0.0, r.sse, 1e-12)
    }

    @Test
    fun `custom prior converges to truth with enough data`() {
        // A confident-but-wrong prior should still be overridden by data.
        val truth = doubleArrayOf(0.5, -1.0, 0.7)
        val wrongMean = DenseVector.of(doubleArrayOf(-2.0, 3.0, -1.0))
        val blr = BayesianLinearRegression(
            featureSize = 3,
            priorMean = wrongMean,
            priorCovariance = DenseMatrix.diagonal(3, 0.1),
        )
        val rng = Random(42)
        repeat(5000) {
            val x = DoubleArray(3) { rng.nextDouble() * 2 - 1 }
            var y = 0.0
            for (i in 0 until 3) y += truth[i] * x[i]
            blr.update(x, y, 1.0)
        }
        val w = blr.read().weights
        for (i in 0 until 3) {
            assertTrue(abs(w[i] - truth[i]) < 0.1, "w[$i]=${w[i]} far from ${truth[i]}")
        }
    }

    @Test
    fun `merge equals sequential update for custom prior`() {
        // Property: merging two instances trained on disjoint data should agree with
        // a single instance trained on the union - independent of the prior, as long
        // as both branches start from the same prior.
        val priorMean = DenseVector.of(doubleArrayOf(0.2, -0.1))
        val priorCov = DenseMatrix.of(
            arrayOf(
                doubleArrayOf(0.5, 0.1),
                doubleArrayOf(0.1, 0.5),
            )
        )
        fun fresh() = BayesianLinearRegression(
            featureSize = 2,
            priorMean = priorMean,
            priorCovariance = priorCov,
        )

        val rng = Random(123)
        val obs = (0 until 400).map {
            val x = DoubleArray(2) { rng.nextDouble() * 2 - 1 }
            val y = 0.7 * x[0] - 0.3 * x[1]
            x to y
        }
        val combined = fresh()
        obs.forEach { (x, y) -> combined.update(x, y, 1.0) }

        val a = fresh()
        val b = fresh()
        obs.take(200).forEach { (x, y) -> a.update(x, y, 1.0) }
        obs.drop(200).forEach { (x, y) -> b.update(x, y, 1.0) }
        a.merge(b.read())

        val rc = combined.read()
        val ra = a.read()
        for (i in 0 until 2) assertTrue(
            abs(rc.weights[i] - ra.weights[i]) < 0.05,
            "merge diverged at $i: ${rc.weights[i]} vs ${ra.weights[i]}"
        )
    }

    @Test
    fun `fitPopulationPrior returns the simple mean for identical posteriors`() {
        val mean = doubleArrayOf(1.0, -0.5)
        val cov = DenseMatrix.diagonal(2, 0.4)
        val snap = CovarianceRegressionResult(
            weights = DenseVector.of(mean),
            bias = 0.0,
            biasPrecision = 1.0,
            totalWeights = 100.0,
            step = 100L,
            covariance = DenseMatrix.of(cov.toArray()),
            covarianceL = cov.let { c -> DenseMatrix.diagonal(2, kotlin.math.sqrt(0.4)) },
            sse = 0.0,
        )
        val prior = BayesianLinearRegression.fitPopulationPrior(List(5) { snap })
        for (i in 0 until 2) {
            assertEquals(mean[i], prior.mean[i], 1e-12)
            // Between-cluster variance is zero, within-cluster variance is `cov`.
            assertEquals(cov[i, i], prior.covariance[i, i], 1e-12)
        }
        assertEquals(5, prior.instanceCount)
    }

    @Test
    fun `fitPopulationPrior captures between-instance spread`() {
        // Two posteriors with very different means and tight covariance: the population
        // covariance should be dominated by the between-instance term (~ (mu_diff/2)^2).
        val tight = DenseMatrix.diagonal(2, 0.01)
        val sqrt01 = kotlin.math.sqrt(0.01)
        fun snap(mu: DoubleArray) = CovarianceRegressionResult(
            weights = DenseVector.of(mu),
            bias = 0.0,
            biasPrecision = 1.0,
            totalWeights = 100.0,
            step = 100L,
            covariance = DenseMatrix.of(tight.toArray()),
            covarianceL = DenseMatrix.diagonal(2, sqrt01),
            sse = 0.0,
        )
        val a = snap(doubleArrayOf(1.0, 0.0))
        val b = snap(doubleArrayOf(-1.0, 0.0))
        val prior = BayesianLinearRegression.fitPopulationPrior(listOf(a, b))
        assertEquals(0.0, prior.mean[0], 1e-12)
        assertEquals(0.0, prior.mean[1], 1e-12)
        // Between = 0.5 * (1 - 0)^2 + 0.5 * (-1 - 0)^2 = 1.0; within = 0.01.
        assertTrue(abs(prior.covariance[0, 0] - 1.01) < 1e-9, "got ${prior.covariance[0, 0]}")
        assertTrue(abs(prior.covariance[1, 1] - 0.01) < 1e-9, "got ${prior.covariance[1, 1]}")
    }

    @Test
    fun `fitPopulationPrior result seeds a new BLR`() {
        // The output is exactly the shape BLR's constructor wants - feed it straight in
        // and confirm the new instance starts at the fitted population mean.
        val cov = DenseMatrix.diagonal(2, 0.5)
        val sqrt05 = kotlin.math.sqrt(0.5)
        val snaps = listOf(
            doubleArrayOf(0.5, 1.0),
            doubleArrayOf(0.3, 0.8),
            doubleArrayOf(0.4, 1.2),
        ).map {
            CovarianceRegressionResult(
                weights = DenseVector.of(it),
                bias = 0.0,
                biasPrecision = 1.0,
                totalWeights = 50.0,
                step = 50L,
                covariance = DenseMatrix.of(cov.toArray()),
                covarianceL = DenseMatrix.diagonal(2, sqrt05),
                sse = 0.0,
            )
        }
        val prior = BayesianLinearRegression.fitPopulationPrior(snaps)
        val seeded = BayesianLinearRegression(
            featureSize = 2,
            priorMean = prior.mean,
            priorCovariance = prior.covariance,
        )
        val r = seeded.read()
        assertEquals(prior.mean[0], r.weights[0], 1e-12)
        assertEquals(prior.mean[1], r.weights[1], 1e-12)
        for (i in 0 until 2) for (j in 0 until 2)
            assertEquals(prior.covariance[i, j], r.covariance[i, j], 1e-9)
    }

    @Test
    fun `fitPopulationPrior throws on empty input`() {
        assertFailsWith<IllegalArgumentException> {
            BayesianLinearRegression.fitPopulationPrior(emptyList())
        }
    }

    @Test
    fun `fitPopulationPrior honours custom weight selector`() {
        // With weight selector returning 1.0 for the first snapshot and 0.0 for the
        // second, the result should equal the first snapshot in isolation.
        val cov = DenseMatrix.diagonal(2, 0.3)
        val sqrt03 = kotlin.math.sqrt(0.3)
        fun snap(mu: DoubleArray) = CovarianceRegressionResult(
            weights = DenseVector.of(mu),
            bias = 0.0,
            biasPrecision = 1.0,
            totalWeights = 1.0,
            step = 1L,
            covariance = DenseMatrix.of(cov.toArray()),
            covarianceL = DenseMatrix.diagonal(2, sqrt03),
            sse = 0.0,
        )
        val a = snap(doubleArrayOf(2.0, -2.0))
        val b = snap(doubleArrayOf(-2.0, 2.0))
        val list = listOf(a, b)
        var i = 0
        val prior = BayesianLinearRegression.fitPopulationPrior(list) { _ ->
            val w = if (i == 0) 1.0 else 1e-12
            i++
            w
        }
        assertTrue(abs(prior.mean[0] - 2.0) < 1e-6, "got ${prior.mean[0]}")
        assertTrue(abs(prior.mean[1] - -2.0) < 1e-6, "got ${prior.mean[1]}")
    }
}
