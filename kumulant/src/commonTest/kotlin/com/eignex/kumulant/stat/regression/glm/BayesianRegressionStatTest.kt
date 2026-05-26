package com.eignex.kumulant.stat.regression.glm

import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.math.DenseVector
import kotlin.math.abs
import kotlin.math.exp
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
class BayesianRegressionStatTest {

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
    fun `bayesian should recover ground truth and shrink covariance`() {
        val stat = BayesianRegressionStat(featureSize = 3, priorVariance = 1.0)
        val truth = doubleArrayOf(0.8, 1.2, -0.5)
        fitLine(stat, truth, intercept = 0.0)
        val r = stat.read()
        for (i in truth.indices) {
            assertTrue(
                abs(r.weights[i] - truth[i]) < 0.1,
                "weight[$i]=${r.weights[i]} far from truth=${truth[i]}",
            )
        }
        for (i in truth.indices) {
            assertTrue(
                r.covariance[i, i] < 0.05,
                "Sum[$i,$i]=${r.covariance[i, i]} did not shrink from prior 1.0",
            )
        }
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
                "merged weight[$i]=${merged.weights[i]} far from truth=${truth[i]}",
            )
            // Posterior product should land in the same neighbourhood as replaying
            // all observations into one stat - not pointwise-identical because SMW
            // accumulates a slightly different trajectory.
            assertTrue(
                abs(merged.weights[i] - refResult.weights[i]) < 0.15,
                "merged weight[$i]=${merged.weights[i]} diverged from replay=${refResult.weights[i]}",
            )
        }
        assertEquals(4000.0, merged.totalWeights, absoluteTolerance = 1e-9)
        // Combined posterior should be at least as tight as each operand.
        for (i in truth.indices) {
            assertTrue(
                merged.covariance[i, i] < 0.05,
                "merged Sum[$i,$i]=${merged.covariance[i, i]} did not tighten",
            )
        }
    }

    @Test
    fun `Identity link is the default and predict returns the linear predictor`() {
        val stat = BayesianRegressionStat(featureSize = 2)
        assertEquals(Link.Identity, stat.link)
        val snap = stat.read()
        assertEquals(Link.Identity, snap.link)
        val x = DenseVector.of(doubleArrayOf(0.5, -0.3))
        assertEquals(snap.linearPredictor(x), snap.predict(x))
    }

    @Test
    fun `Logit link learns a binary classifier from simulated 0_1 data`() {
        val rng = Random(7)
        val stat = BayesianRegressionStat(featureSize = 2, priorVariance = 1.0, link = Link.Logit)
        repeat(2000) {
            val x = doubleArrayOf(rng.nextDouble() * 2 - 1, rng.nextDouble() * 2 - 1)
            val logit = 2.0 * x[0] - 1.0 * x[1]
            val p = 1.0 / (1.0 + exp(-logit))
            val y = if (rng.nextDouble() < p) 1.0 else 0.0
            stat.update(x, y, 1.0)
        }
        val snap = stat.read()
        val p1 = snap.predict(DenseVector.of(doubleArrayOf(1.0, 0.0)))
        assertTrue(p1 in 0.0..1.0, "predict returned $p1, expected probability")
        assertTrue(p1 > 0.7, "predicted P(positive | (1,0)) = $p1, expected > 0.7")
        val pNeg = snap.predict(DenseVector.of(doubleArrayOf(-1.0, 1.0)))
        assertTrue(pNeg < 0.3, "predicted P(positive | (-1,1)) = $pNeg, expected < 0.3")
    }

    @Test
    fun `Log link learns a Poisson rate predictor`() {
        val rng = Random(2)
        val stat = BayesianRegressionStat(featureSize = 2, priorVariance = 1.0, link = Link.Log)
        repeat(2000) {
            val x = doubleArrayOf(rng.nextDouble() * 0.5, rng.nextDouble() * 0.5)
            val rate = exp(1.0 * x[0] + 0.5 * x[1])
            stat.update(x, rate, 1.0)
        }
        val snap = stat.read()
        val x = DenseVector.of(doubleArrayOf(0.5, 0.5))
        val pred = snap.predict(x)
        val expected = exp(0.75)
        assertTrue(abs(pred - expected) / expected < 0.3, "pred=$pred, expected $expected")
    }

    @Test
    fun `Logit predict roundtrip through inverse-link is sigmoid of linear predictor`() {
        val stat = BayesianRegressionStat(featureSize = 2, link = Link.Logit)
        stat.update(doubleArrayOf(1.0, 0.5), 1.0, 5.0)
        val snap = stat.read()
        val x = DenseVector.of(doubleArrayOf(0.7, -0.3))
        val eta = snap.linearPredictor(x)
        val expected = 1.0 / (1.0 + exp(-eta))
        assertEquals(expected, snap.predict(x), absoluteTolerance = 1e-12)
    }

    @Test
    fun `Log predict roundtrip through inverse-link is exp of linear predictor`() {
        val stat = BayesianRegressionStat(featureSize = 2, link = Link.Log)
        stat.update(doubleArrayOf(0.3, 0.4), 2.0, 1.0)
        val snap = stat.read()
        val x = DenseVector.of(doubleArrayOf(0.1, 0.2))
        val eta = snap.linearPredictor(x)
        assertEquals(exp(eta), snap.predict(x), absoluteTolerance = 1e-12)
    }
}
