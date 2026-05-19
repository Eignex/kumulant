package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.RegressionStat
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Smoke tests for the multivariate regression stats: feed each implementation a
 * known linear ground truth and check that [RegressionStat.read] recovers it within
 * a slack tolerance.
 */
class LinearRegressionStatsTest {

    private fun fitLine(
        stat: RegressionStat<*>,
        slope: DoubleArray,
        intercept: Double,
        n: Int = 4000,
        seed: Long = 42L,
    ) {
        val rng = Random(seed)
        repeat(n) {
            val x = DoubleArray(slope.size) { rng.nextDouble() * 2.0 - 1.0 }
            var y = intercept
            for (i in slope.indices) y += slope[i] * x[i]
            y += rng.nextDouble() * 0.02 - 0.01 // small noise
            stat.update(x, y, 1.0)
        }
    }

    @Test
    fun `sgd should recover ground truth weights`() {
        val stat = StochasticRegressionStat(featureSize = 3, learningRate = ConstantRate(0.05))
        val truth = doubleArrayOf(1.5, -2.0, 0.5)
        fitLine(stat, truth, intercept = 0.3)
        val r = stat.read()
        for (i in truth.indices) assertTrue(
            abs(r.weights[i] - truth[i]) < 0.1,
            "weight[$i]=${r.weights[i]} far from truth=${truth[i]}"
        )
        assertTrue(abs(r.bias - 0.3) < 0.1, "bias=${r.bias} far from 0.3")
    }

    @Test
    fun `diagonal should recover ground truth weights with finite per-coefficient precision`() {
        val stat = DiagonalRegressionStat(featureSize = 3, priorPrecision = 0.01)
        val truth = doubleArrayOf(1.0, -1.5, 2.0)
        fitLine(stat, truth, intercept = -0.2)
        val r = stat.read()
        for (i in truth.indices) assertTrue(
            abs(r.weights[i] - truth[i]) < 0.05,
            "weight[$i]=${r.weights[i]} far from truth=${truth[i]}"
        )
        val precisionArr = r.precision.toDoubleArray()
        assertTrue(precisionArr.all { it > 100.0 }, "precision should grow with data: ${precisionArr.toList()}")
        assertTrue(r.biasPrecision > 100.0)
    }

    @Test
    fun `bayesian should recover ground truth and shrink covariance`() {
        val stat = BayesianRegressionStat(featureSize = 3, priorVariance = 1.0)
        val truth = doubleArrayOf(0.8, 1.2, -0.5)
        fitLine(stat, truth, intercept = 0.0)
        val r = stat.read()
        for (i in truth.indices) assertTrue(
            abs(r.weights[i] - truth[i]) < 0.1,
            "weight[$i]=${r.weights[i]} far from truth=${truth[i]}"
        )
        for (i in truth.indices) assertTrue(
            r.covariance[i, i] < 0.05,
            "Sum[$i,$i]=${r.covariance[i, i]} did not shrink from prior 1.0"
        )
    }

    @Test
    fun `merge on diagonal regression combines independent normals`() {
        val a = DiagonalRegressionStat(featureSize = 2, priorPrecision = 0.01)
        val b = DiagonalRegressionStat(featureSize = 2, priorPrecision = 0.01)
        val truth = doubleArrayOf(2.0, -1.0)
        fitLine(a, truth, intercept = 0.0, n = 2000, seed = 1L)
        fitLine(b, truth, intercept = 0.0, n = 2000, seed = 2L)
        a.merge(b.read())
        val r = a.read()
        assertEquals(4000.0, r.totalWeights, absoluteTolerance = 1e-9)
        for (i in truth.indices) assertTrue(abs(r.weights[i] - truth[i]) < 0.05)
    }

    @Test
    fun `merge on SGD blends sample-weighted`() {
        val a = StochasticRegressionStat(featureSize = 2, learningRate = ConstantRate(0.05))
        val b = StochasticRegressionStat(featureSize = 2, learningRate = ConstantRate(0.05))
        val truth = doubleArrayOf(1.0, -1.0)
        fitLine(a, truth, intercept = 0.0, n = 2000, seed = 11L)
        fitLine(b, truth, intercept = 0.0, n = 2000, seed = 22L)
        a.merge(b.read())
        val r = a.read()
        assertEquals(4000.0, r.totalWeights, absoluteTolerance = 1e-9)
        for (i in truth.indices) assertTrue(
            abs(r.weights[i] - truth[i]) < 0.15,
            "merged weight[$i]=${r.weights[i]} far from truth=${truth[i]}"
        )
    }

    @Test
    fun `merge on Bayesian combines posteriors via density product`() {
        val a = BayesianRegressionStat(featureSize = 3, priorVariance = 1.0)
        val b = BayesianRegressionStat(featureSize = 3, priorVariance = 1.0)
        val truth = doubleArrayOf(0.5, -1.2, 0.9)
        fitLine(a, truth, intercept = 0.0, n = 2000, seed = 5L)
        fitLine(b, truth, intercept = 0.0, n = 2000, seed = 7L)

        // Reference: a single stat fed the same total data should agree with merge(a, b).
        val ref = BayesianRegressionStat(featureSize = 3, priorVariance = 1.0)
        fitLine(ref, truth, intercept = 0.0, n = 2000, seed = 5L)
        fitLine(ref, truth, intercept = 0.0, n = 2000, seed = 7L)

        a.merge(b.read())
        val merged = a.read()
        val refResult = ref.read()

        for (i in truth.indices) {
            assertTrue(
                abs(merged.weights[i] - truth[i]) < 0.1,
                "merged weight[$i]=${merged.weights[i]} far from truth=${truth[i]}"
            )
            // Posterior product should land in the same neighbourhood as replaying
            // all observations into one stat - not pointwise-identical because SMW
            // accumulates a slightly different trajectory.
            assertTrue(
                abs(merged.weights[i] - refResult.weights[i]) < 0.15,
                "merged weight[$i]=${merged.weights[i]} diverged from replay=${refResult.weights[i]}"
            )
        }
        assertEquals(4000.0, merged.totalWeights, absoluteTolerance = 1e-9)
        // Combined posterior should be at least as tight as each operand.
        for (i in truth.indices) assertTrue(
            merged.covariance[i, i] < 0.05,
            "merged Sum[$i,$i]=${merged.covariance[i, i]} did not tighten"
        )
    }

    @Test
    fun `reset restores prior state`() {
        val stat = DiagonalRegressionStat(featureSize = 2, priorPrecision = 0.5)
        fitLine(stat, doubleArrayOf(1.0, 1.0), intercept = 0.0, n = 100)
        stat.reset()
        val r = stat.read()
        assertEquals(0.0, r.bias)
        assertEquals(0.0, r.totalWeights)
        assertEquals(0L, r.step)
        assertTrue(r.weights.toDoubleArray().all { it == 0.0 })
        assertTrue(r.precision.toDoubleArray().all { it == 0.5 })
    }

    @Test
    fun `featureSize mismatch on update throws`() {
        val stat = StochasticRegressionStat(featureSize = 3)
        assertFailsWith<IllegalArgumentException> {
            stat.update(doubleArrayOf(1.0, 2.0), y = 0.0)
        }
    }

    // === GLM Link parameter tests ===

    @Test
    fun `Identity link is the default and predict returns the linear predictor`() {
        val stat = BayesianRegressionStat(featureSize = 2)
        assertEquals(Link.Identity, stat.link)
        val snap = stat.read()
        assertEquals(Link.Identity, snap.link)
        val x = com.eignex.kumulant.math.DenseVector.of(doubleArrayOf(0.5, -0.3))
        assertEquals(snap.linearPredictor(x), snap.predict(x))
    }

    @Test
    fun `Logit link learns a binary classifier from simulated 0_1 data`() {
        val rng = kotlin.random.Random(7)
        val stat = BayesianRegressionStat(featureSize = 2, priorVariance = 1.0, link = Link.Logit)
        repeat(2000) {
            val x = doubleArrayOf(rng.nextDouble() * 2 - 1, rng.nextDouble() * 2 - 1)
            val logit = 2.0 * x[0] - 1.0 * x[1]
            val p = 1.0 / (1.0 + kotlin.math.exp(-logit))
            val y = if (rng.nextDouble() < p) 1.0 else 0.0
            stat.update(x, y, 1.0)
        }
        val snap = stat.read()
        val p1 = snap.predict(com.eignex.kumulant.math.DenseVector.of(doubleArrayOf(1.0, 0.0)))
        assertTrue(p1 in 0.0..1.0, "predict returned $p1, expected probability")
        assertTrue(p1 > 0.7, "predicted P(positive | (1,0)) = $p1, expected > 0.7")
        val pNeg = snap.predict(com.eignex.kumulant.math.DenseVector.of(doubleArrayOf(-1.0, 1.0)))
        assertTrue(pNeg < 0.3, "predicted P(positive | (-1,1)) = $pNeg, expected < 0.3")
    }

    @Test
    fun `Log link learns a Poisson rate predictor`() {
        val rng = kotlin.random.Random(2)
        val stat = BayesianRegressionStat(featureSize = 2, priorVariance = 1.0, link = Link.Log)
        repeat(2000) {
            val x = doubleArrayOf(rng.nextDouble() * 0.5, rng.nextDouble() * 0.5)
            val rate = kotlin.math.exp(1.0 * x[0] + 0.5 * x[1])
            stat.update(x, rate, 1.0)
        }
        val snap = stat.read()
        val x = com.eignex.kumulant.math.DenseVector.of(doubleArrayOf(0.5, 0.5))
        val pred = snap.predict(x)
        val expected = kotlin.math.exp(0.75)
        assertTrue(kotlin.math.abs(pred - expected) / expected < 0.3, "pred=$pred, expected $expected")
    }

    @Test
    fun `SGD with Logit link converges on classification data`() {
        val rng = kotlin.random.Random(11)
        val stat = StochasticRegressionStat(
            featureSize = 2,
            learningRate = ConstantRate(0.1),
            link = Link.Logit,
        )
        repeat(3000) {
            val x = doubleArrayOf(rng.nextDouble() * 2 - 1, rng.nextDouble() * 2 - 1)
            val logit = 1.5 * x[0] - 1.5 * x[1]
            val p = 1.0 / (1.0 + kotlin.math.exp(-logit))
            val y = if (rng.nextDouble() < p) 1.0 else 0.0
            stat.update(x, y, 1.0)
        }
        val snap = stat.read()
        assertTrue(snap.weights[0] > 0.0, "w[0] = ${snap.weights[0]} should be positive")
        assertTrue(snap.weights[1] < 0.0, "w[1] = ${snap.weights[1]} should be negative")
        val p1 = snap.predict(com.eignex.kumulant.math.DenseVector.of(doubleArrayOf(1.0, -1.0)))
        assertTrue(p1 in 0.0..1.0)
    }

    @Test
    fun `Diagonal with Logit link tightens precision via curvature`() {
        val rng = kotlin.random.Random(5)
        val stat = DiagonalRegressionStat(featureSize = 2, link = Link.Logit, priorPrecision = 0.5)
        repeat(500) {
            val x = doubleArrayOf(rng.nextDouble(), rng.nextDouble())
            val y = if (rng.nextDouble() < 0.5) 1.0 else 0.0
            stat.update(x, y, 1.0)
        }
        val snap = stat.read()
        assertTrue(snap.precision[0] > 0.5, "precision[0] = ${snap.precision[0]}")
        assertTrue(snap.precision[1] > 0.5, "precision[1] = ${snap.precision[1]}")
    }

    @Test
    fun `Logit predict roundtrip through inverse-link is sigmoid of linear predictor`() {
        val stat = BayesianRegressionStat(featureSize = 2, link = Link.Logit)
        stat.update(doubleArrayOf(1.0, 0.5), 1.0, 5.0)
        val snap = stat.read()
        val x = com.eignex.kumulant.math.DenseVector.of(doubleArrayOf(0.7, -0.3))
        val eta = snap.linearPredictor(x)
        val expected = 1.0 / (1.0 + kotlin.math.exp(-eta))
        assertEquals(expected, snap.predict(x), absoluteTolerance = 1e-12)
    }

    @Test
    fun `Log predict roundtrip through inverse-link is exp of linear predictor`() {
        val stat = BayesianRegressionStat(featureSize = 2, link = Link.Log)
        stat.update(doubleArrayOf(0.3, 0.4), 2.0, 1.0)
        val snap = stat.read()
        val x = com.eignex.kumulant.math.DenseVector.of(doubleArrayOf(0.1, 0.2))
        val eta = snap.linearPredictor(x)
        assertEquals(kotlin.math.exp(eta), snap.predict(x), absoluteTolerance = 1e-12)
    }

    // === Link loss correctness ===

    @Test
    fun `Identity loss equals squared error`() {
        val link = Link.Identity
        assertEquals(0.0, link.loss(eta = 1.0, y = 1.0))
        assertEquals(4.0, link.loss(eta = 3.0, y = 1.0))
    }

    @Test
    fun `Logit loss matches softplus form`() {
        val link = Link.Logit
        val eta = 1.5
        val expectedAtOne = kotlin.math.ln(1.0 + kotlin.math.exp(eta)) - 1.0 * eta
        val expectedAtZero = kotlin.math.ln(1.0 + kotlin.math.exp(eta))
        assertEquals(expectedAtOne, link.loss(eta, y = 1.0), absoluteTolerance = 1e-12)
        assertEquals(expectedAtZero, link.loss(eta, y = 0.0), absoluteTolerance = 1e-12)
    }

    @Test
    fun `Logit loss is numerically stable at large positive eta`() {
        val link = Link.Logit
        val loss = link.loss(eta = 1000.0, y = 0.0)
        assertTrue(loss.isFinite(), "loss=$loss not finite")
        assertTrue(kotlin.math.abs(loss - 1000.0) < 1e-9, "loss = $loss, expected ~1000")
    }

    @Test
    fun `Log loss is exp eta minus y times eta`() {
        val link = Link.Log
        val eta = 0.5
        assertEquals(kotlin.math.exp(eta) - 2.0 * eta, link.loss(eta, y = 2.0), absoluteTolerance = 1e-12)
    }

    // === HOGWILD concurrency / lazy regularisation tests for SGD ===

    @Test
    fun `SGD under Concurrency Relaxed converges with no penalty`() {
        val rng = kotlin.random.Random(3)
        val stat = StochasticRegressionStat(
            featureSize = 2,
            learningRate = ConstantRate(0.05),
            concurrency = Concurrency.Relaxed,
        )
        repeat(1000) {
            val x = doubleArrayOf(rng.nextDouble() * 2 - 1, rng.nextDouble() * 2 - 1)
            stat.update(x, y = 1.0 * x[0] - 2.0 * x[1], weight = 1.0)
        }
        val snap = stat.read()
        // Relaxed mode is still single-threaded in tests; convergence should match Concurrency.None
        assertTrue(kotlin.math.abs(snap.weights[0] - 1.0) < 0.3, "w[0] = ${snap.weights[0]}")
        assertTrue(kotlin.math.abs(snap.weights[1] - (-2.0)) < 0.3, "w[1] = ${snap.weights[1]}")
    }

    @Test
    fun `SGD with lazy L1 induces sparsity on the irrelevant coordinate`() {
        val rng = kotlin.random.Random(4)
        // lambda small enough that the signal coord's gradient beats the threshold,
        // but not so small that irrelevant coords drift far from zero.
        val stat = StochasticRegressionStat(
            featureSize = 3,
            learningRate = ConstantRate(0.05),
            penalty = Penalty.L1(0.01),
        )
        // Only coord 0 matters; coords 1, 2 are irrelevant noise.
        repeat(2000) {
            val x = doubleArrayOf(rng.nextDouble(), rng.nextDouble(), rng.nextDouble())
            stat.update(x, y = 1.5 * x[0], weight = 1.0)
        }
        val snap = stat.read()
        // Coord 0 keeps signal (target 1.5; L1 shrinks slightly so > 0.5 is comfortable)
        assertTrue(snap.weights[0] > 0.5, "w[0]=${snap.weights[0]} should be substantially positive")
        // L1 should keep irrelevant coords closer to zero than the signal coord
        assertTrue(
            kotlin.math.abs(snap.weights[1]) < snap.weights[0],
            "w[1]=${snap.weights[1]} should be smaller than w[0]=${snap.weights[0]}",
        )
        assertTrue(
            kotlin.math.abs(snap.weights[2]) < snap.weights[0],
            "w[2]=${snap.weights[2]} should be smaller than w[0]=${snap.weights[0]}",
        )
    }

    @Test
    fun `SGD with lazy L2 shrinks weights toward zero`() {
        val rng = kotlin.random.Random(5)
        // Strong L2 -> weights stay small even with strong signal.
        val statL2 = StochasticRegressionStat(
            featureSize = 2,
            learningRate = ConstantRate(0.05),
            penalty = Penalty.L2(0.5),
        )
        val statNoReg = StochasticRegressionStat(
            featureSize = 2,
            learningRate = ConstantRate(0.05),
        )
        repeat(500) {
            val x = doubleArrayOf(rng.nextDouble(), rng.nextDouble())
            statL2.update(x, y = 5.0 * x[0] + 3.0 * x[1], weight = 1.0)
            statNoReg.update(x, y = 5.0 * x[0] + 3.0 * x[1], weight = 1.0)
        }
        val l2 = statL2.read()
        val no = statNoReg.read()
        // L2 weights should be strictly smaller in magnitude (or near zero) than the unregularised baseline
        assertTrue(kotlin.math.abs(l2.weights[0]) <= kotlin.math.abs(no.weights[0]) + 0.05)
        assertTrue(kotlin.math.abs(l2.weights[1]) <= kotlin.math.abs(no.weights[1]) + 0.05)
    }
}
