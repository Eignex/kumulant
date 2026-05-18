package com.eignex.kumulant.stat.regression

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
}
