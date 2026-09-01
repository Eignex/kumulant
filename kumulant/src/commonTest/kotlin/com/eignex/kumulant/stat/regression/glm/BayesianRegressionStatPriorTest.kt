package com.eignex.kumulant.stat.regression.glm

import com.eignex.koblas.core.F64DenseMatrix
import com.eignex.koblas.core.F64DenseVector
import kotlin.math.abs
import kotlin.math.sqrt
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

// The custom prior is what lets klause persist a posterior across solver calls: an earlier read()
// snapshot is fed back in as the next instance's prior.
class BayesianRegressionStatPriorTest {

    @Test
    fun `default prior matches isotropic Gaussian with priorVariance on the diagonal`() {
        val blr = BayesianRegressionStat(featureSize = 3, priorVariance = 2.0)
        val r = blr.read()
        for (i in 0 until 3) {
            assertEquals(0.0, r.weights[i], 1e-12)
            assertEquals(2.0, r.covariance[i, i], 1e-12)
            for (j in 0 until 3) if (i != j) assertEquals(0.0, r.covariance[i, j], 1e-12)
        }
    }

    @Test
    fun `custom prior seeds the initial weights and covariance`() {
        val mean = F64DenseVector.of(doubleArrayOf(0.5, -1.0, 2.0))
        val cov = F64DenseMatrix.of(
            arrayOf(
                doubleArrayOf(1.0, 0.3, 0.0),
                doubleArrayOf(0.3, 1.0, 0.0),
                doubleArrayOf(0.0, 0.0, 0.5),
            ),
        )
        val blr = BayesianRegressionStat(
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
        val bad = F64DenseMatrix.of(
            arrayOf(
                doubleArrayOf(1.0, 0.0),
                doubleArrayOf(0.0, -0.1),
            ),
        )
        assertFailsWith<IllegalArgumentException> {
            BayesianRegressionStat(featureSize = 2, priorCovariance = bad)
        }
    }

    @Test
    fun `reset restores configured prior`() {
        val mean = F64DenseVector.of(doubleArrayOf(1.0, -0.5))
        val cov = F64DenseMatrix.diagonal(2, 0.25)
        val blr = BayesianRegressionStat(featureSize = 2, priorMean = mean, priorCovariance = cov)
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
        val wrongMean = F64DenseVector.of(doubleArrayOf(-2.0, 3.0, -1.0))
        val blr = BayesianRegressionStat(
            featureSize = 3,
            priorMean = wrongMean,
            priorCovariance = F64DenseMatrix.diagonal(3, 0.1),
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
        val priorMean = F64DenseVector.of(doubleArrayOf(0.2, -0.1))
        val priorCov = F64DenseMatrix.of(
            arrayOf(
                doubleArrayOf(0.5, 0.1),
                doubleArrayOf(0.1, 0.5),
            ),
        )
        fun fresh() = BayesianRegressionStat(
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
        for (i in 0 until 2) {
            assertTrue(
                abs(rc.weights[i] - ra.weights[i]) < 0.05,
                "merge diverged at $i: ${rc.weights[i]} vs ${ra.weights[i]}",
            )
        }
    }

    @Test
    fun `merging an empty custom prior state preserves the populated posterior`() {
        val priorMean = F64DenseVector.of(doubleArrayOf(0.4, -0.7))
        val priorCovariance = F64DenseMatrix.of(
            arrayOf(
                doubleArrayOf(0.8, 0.2),
                doubleArrayOf(0.2, 0.6),
            ),
        )
        fun fresh() = BayesianRegressionStat(
            featureSize = 2,
            priorMean = priorMean,
            priorCovariance = priorCovariance,
        )
        val populated = fresh().also {
            it.update(doubleArrayOf(1.0, -0.25), 0.5)
            it.update(doubleArrayOf(-0.5, 1.0), -1.0)
        }
        val expected = populated.read()

        populated.merge(fresh().read())

        val actual = populated.read()
        for (i in 0 until 2) {
            assertEquals(expected.weights[i], actual.weights[i], 1e-12)
            for (j in 0 until 2) assertEquals(expected.covariance[i, j], actual.covariance[i, j], 1e-12)
        }
    }

    @Test
    fun `fitPopulationPrior returns the simple mean for identical posteriors`() {
        val mean = doubleArrayOf(1.0, -0.5)
        val cov = F64DenseMatrix.diagonal(2, 0.4)
        val snap = CovarianceRegressionResult(
            weights = F64DenseVector.of(mean),
            bias = 0.0,
            biasPrecision = 1.0,
            totalWeights = 100.0,
            step = 100L,
            covariance = F64DenseMatrix.of(cov.toArray()),
            covarianceL = cov.let { c -> F64DenseMatrix.diagonal(2, sqrt(0.4)) },
            sse = 0.0,
        )
        val prior = BayesianRegressionStat.fitPopulationPrior(List(5) { snap })
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
        val tight = F64DenseMatrix.diagonal(2, 0.01)
        val sqrt01 = sqrt(0.01)
        fun snap(mu: DoubleArray) = CovarianceRegressionResult(
            weights = F64DenseVector.of(mu),
            bias = 0.0,
            biasPrecision = 1.0,
            totalWeights = 100.0,
            step = 100L,
            covariance = F64DenseMatrix.of(tight.toArray()),
            covarianceL = F64DenseMatrix.diagonal(2, sqrt01),
            sse = 0.0,
        )
        val a = snap(doubleArrayOf(1.0, 0.0))
        val b = snap(doubleArrayOf(-1.0, 0.0))
        val prior = BayesianRegressionStat.fitPopulationPrior(listOf(a, b))
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
        val cov = F64DenseMatrix.diagonal(2, 0.5)
        val sqrt05 = sqrt(0.5)
        val snaps = listOf(
            doubleArrayOf(0.5, 1.0),
            doubleArrayOf(0.3, 0.8),
            doubleArrayOf(0.4, 1.2),
        ).map {
            CovarianceRegressionResult(
                weights = F64DenseVector.of(it),
                bias = 0.0,
                biasPrecision = 1.0,
                totalWeights = 50.0,
                step = 50L,
                covariance = F64DenseMatrix.of(cov.toArray()),
                covarianceL = F64DenseMatrix.diagonal(2, sqrt05),
                sse = 0.0,
            )
        }
        val prior = BayesianRegressionStat.fitPopulationPrior(snaps)
        val seeded = BayesianRegressionStat(
            featureSize = 2,
            priorMean = prior.mean,
            priorCovariance = prior.covariance,
        )
        val r = seeded.read()
        assertEquals(prior.mean[0], r.weights[0], 1e-12)
        assertEquals(prior.mean[1], r.weights[1], 1e-12)
        for (i in 0 until 2) {
            for (j in 0 until 2) {
                assertEquals(prior.covariance[i, j], r.covariance[i, j], 1e-9)
            }
        }
    }

    @Test
    fun `fitPopulationPrior throws on empty input`() {
        assertFailsWith<IllegalArgumentException> {
            BayesianRegressionStat.fitPopulationPrior(emptyList())
        }
    }

    @Test
    fun `fitPopulationPrior honours custom weight selector`() {
        // With weight selector returning 1.0 for the first snapshot and 0.0 for the
        // second, the result should equal the first snapshot in isolation.
        val cov = F64DenseMatrix.diagonal(2, 0.3)
        val sqrt03 = sqrt(0.3)
        fun snap(mu: DoubleArray) = CovarianceRegressionResult(
            weights = F64DenseVector.of(mu),
            bias = 0.0,
            biasPrecision = 1.0,
            totalWeights = 1.0,
            step = 1L,
            covariance = F64DenseMatrix.of(cov.toArray()),
            covarianceL = F64DenseMatrix.diagonal(2, sqrt03),
            sse = 0.0,
        )
        val a = snap(doubleArrayOf(2.0, -2.0))
        val b = snap(doubleArrayOf(-2.0, 2.0))
        val list = listOf(a, b)
        var i = 0
        val prior = BayesianRegressionStat.fitPopulationPrior(list) { _ ->
            val w = if (i == 0) 1.0 else 1e-12
            i++
            w
        }
        assertTrue(abs(prior.mean[0] - 2.0) < 1e-6, "got ${prior.mean[0]}")
        assertTrue(abs(prior.mean[1] - -2.0) < 1e-6, "got ${prior.mean[1]}")
    }

    @Test
    fun `fitPopulationPrior with a single snapshot has zero between-instance variance`() {
        val cov = F64DenseMatrix.diagonal(2, 0.4)
        val snap = CovarianceRegressionResult(
            weights = F64DenseVector.of(doubleArrayOf(0.7, -0.3)),
            bias = 0.0,
            biasPrecision = 1.0,
            totalWeights = 50.0,
            step = 50L,
            covariance = F64DenseMatrix.of(cov.toArray()),
            covarianceL = F64DenseMatrix.diagonal(2, sqrt(0.4)),
            sse = 0.0,
        )
        val prior = BayesianRegressionStat.fitPopulationPrior(listOf(snap))
        assertEquals(1, prior.instanceCount)
        assertEquals(0.7, prior.mean[0], 1e-12)
        assertEquals(-0.3, prior.mean[1], 1e-12)
        // Single instance -> between term is 0, population covariance == within covariance.
        for (i in 0 until 2) {
            for (j in 0 until 2) {
                assertEquals(cov[i, j], prior.covariance[i, j], 1e-12)
            }
        }
    }

    @Test
    fun `create propagates the configured prior to the clone`() {
        val mean = F64DenseVector.of(doubleArrayOf(0.4, 1.1))
        val cov = F64DenseMatrix.diagonal(2, 0.3)
        val original = BayesianRegressionStat(
            featureSize = 2,
            priorMean = mean,
            priorCovariance = cov,
        )
        // Mutate the original; the clone should still spring from the configured prior.
        original.update(doubleArrayOf(1.0, 0.0), 5.0, 1.0)
        val clone = original.create(null)
        val r = clone.read()
        for (i in 0 until 2) {
            assertEquals(mean[i], r.weights[i], 1e-12)
            assertEquals(cov[i, i], r.covariance[i, i], 1e-12)
        }
    }

    @Test
    fun `priorMean size mismatch is rejected`() {
        assertFailsWith<IllegalArgumentException> {
            BayesianRegressionStat(
                featureSize = 3,
                priorMean = F64DenseVector.of(doubleArrayOf(1.0, 2.0)),
            )
        }
    }

    @Test
    fun `priorCovariance shape mismatch is rejected`() {
        assertFailsWith<IllegalArgumentException> {
            BayesianRegressionStat(
                featureSize = 3,
                priorCovariance = F64DenseMatrix.diagonal(2, 1.0),
            )
        }
    }

    @Test
    fun `strong prior dominates with very little data`() {
        // 5 noisy observations vs a very tight prior at the wrong mean: posterior
        // should sit much closer to the prior than to the data-only UnivariateRegression() fit.
        val priorMean = F64DenseVector.of(doubleArrayOf(0.0, 0.0))
        val tight = F64DenseMatrix.diagonal(2, 1e-4)
        val blr = BayesianRegressionStat(
            featureSize = 2,
            priorMean = priorMean,
            priorCovariance = tight,
        )
        val rng = Random(0)
        repeat(5) {
            val x = DoubleArray(2) { rng.nextDouble() * 2 - 1 }
            blr.update(x, 5.0 * x[0] + 3.0 * x[1], 1.0)
        }
        val w = blr.read().weights
        for (i in 0 until 2) {
            assertTrue(
                abs(w[i]) < 0.1,
                "strong prior should pin w[$i] near 0, got ${w[i]}",
            )
        }
    }

    @Test
    fun `PopulationPrior round-trips through JSON`() {
        val snap = CovarianceRegressionResult(
            weights = F64DenseVector.of(doubleArrayOf(0.5, -0.5)),
            bias = 0.0,
            biasPrecision = 1.0,
            totalWeights = 10.0,
            step = 10L,
            covariance = F64DenseMatrix.diagonal(2, 0.5),
            covarianceL = F64DenseMatrix.diagonal(2, sqrt(0.5)),
            sse = 0.0,
        )
        val prior = BayesianRegressionStat.fitPopulationPrior(listOf(snap, snap))
        val json = kotlinx.serialization.json.Json
        val wire = json.encodeToString(PopulationPrior.serializer(), prior)
        val decoded = json.decodeFromString(PopulationPrior.serializer(), wire)
        assertEquals(prior, decoded)
    }
}
