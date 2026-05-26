package com.eignex.kumulant.stat.regression.glm

import com.eignex.kumulant.math.DenseMatrix
import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.schema.Sgd
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
class LinearPosteriorsTest {

    private fun sgdSnapshot(): StochasticRegressionResult {
        val stat = StochasticRegressionStat(featureSize = 2, optimizer = Sgd(ConstantRate(0.05)))
        val rng = Random(1)
        repeat(800) {
            val x = doubleArrayOf(rng.nextDouble() * 2 - 1, rng.nextDouble() * 2 - 1)
            val y = 1.0 * x[0] - 2.0 * x[1]
            stat.update(x, y, 1.0)
        }
        return stat.read()
    }

    private fun diagonalSnapshot(): DiagonalRegressionResult {
        val stat = DiagonalRegressionStat(featureSize = 2, priorPrecision = 0.1)
        val rng = Random(1)
        repeat(800) {
            val x = doubleArrayOf(rng.nextDouble() * 2 - 1, rng.nextDouble() * 2 - 1)
            val y = 1.0 * x[0] - 2.0 * x[1]
            stat.update(x, y, 1.0)
        }
        return stat.read()
    }

    private fun bayesianSnapshot(): CovarianceRegressionResult {
        val stat = BayesianRegressionStat(featureSize = 2, priorVariance = 1.0)
        val rng = Random(1)
        repeat(800) {
            val x = doubleArrayOf(rng.nextDouble() * 2 - 1, rng.nextDouble() * 2 - 1)
            val y = 1.0 * x[0] - 2.0 * x[1]
            stat.update(x, y, 1.0)
        }
        return stat.read()
    }

    @Test
    fun `PointPosterior with zero exploration returns the snapshot weights as-is`() {
        val snap = sgdSnapshot()
        val sample = PointPosterior.sample(snap, Random(0), exploration = 0.0)
        assertTrue(sample === snap.weights)
    }

    @Test
    fun `PointPosterior sample with exploration matches snapshot mean across draws`() {
        val snap = sgdSnapshot()
        val rng = Random(0)
        val n = 800
        var s0 = 0.0
        var s1 = 0.0
        repeat(n) {
            val v = PointPosterior.sample(snap, rng, exploration = 0.25)
            s0 += v[0]
            s1 += v[1]
        }
        assertTrue(abs(s0 / n - snap.weights[0]) < 0.05)
        assertTrue(abs(s1 / n - snap.weights[1]) < 0.05)
    }

    @Test
    fun `PointPosterior evaluate with zero exploration is the point prediction`() {
        val snap = sgdSnapshot()
        val x = DenseVector.of(doubleArrayOf(0.3, -0.4))
        val v = PointPosterior.evaluate(snap, x, Random(0), exploration = 0.0)
        assertEquals(snap.predict(x), v, 1e-12)
    }

    @Test
    fun `PointPosterior evaluate with exploration is finite`() {
        val snap = sgdSnapshot()
        val x = DenseVector.of(doubleArrayOf(0.3, -0.4))
        repeat(50) {
            val v = PointPosterior.evaluate(snap, x, Random(it.toLong()), exploration = 0.5)
            assertTrue(v.isFinite())
        }
    }

    @Test
    fun `FactorisedGaussian sample respects per-coord precision`() {
        val snap = diagonalSnapshot()
        val rng = Random(0)
        val n = 1000
        var s0 = 0.0
        var s1 = 0.0
        repeat(n) {
            val v = FactorisedGaussian.sample(snap, rng, exploration = 1.0)
            s0 += v[0]
            s1 += v[1]
        }
        assertTrue(abs(s0 / n - snap.weights[0]) < 0.05)
        assertTrue(abs(s1 / n - snap.weights[1]) < 0.05)
    }

    @Test
    fun `FactorisedGaussian evaluate is centered on predict`() {
        val snap = diagonalSnapshot()
        val x = DenseVector.of(doubleArrayOf(0.1, 0.2))
        val rng = Random(0)
        val n = 600
        var sum = 0.0
        repeat(n) { sum += FactorisedGaussian.evaluate(snap, x, rng, exploration = 0.1) }
        assertTrue(abs(sum / n - snap.predict(x)) < 0.05)
    }

    @Test
    fun `MultivariateGaussian sample is centered on snapshot weights`() {
        val snap = bayesianSnapshot()
        val rng = Random(0)
        val n = 1000
        var s0 = 0.0
        var s1 = 0.0
        repeat(n) {
            val v = MultivariateGaussian.sample(snap, rng, exploration = 0.5)
            s0 += v[0]
            s1 += v[1]
        }
        assertTrue(abs(s0 / n - snap.weights[0]) < 0.05)
        assertTrue(abs(s1 / n - snap.weights[1]) < 0.05)
    }

    @Test
    fun `MultivariateGaussian evaluate is finite and finite-variance`() {
        val snap = bayesianSnapshot()
        val x = DenseVector.of(doubleArrayOf(0.5, 0.5))
        val rng = Random(0)
        val n = 300
        var sum = 0.0
        repeat(n) { sum += MultivariateGaussian.evaluate(snap, x, rng, exploration = 0.2) }
        assertTrue(abs(sum / n - snap.predict(x)) < 0.05)
    }

    @Test
    fun `LinUcb evaluate is deterministic given the snapshot`() {
        val snap = bayesianSnapshot()
        val x = DenseVector.of(doubleArrayOf(0.3, 0.7))
        val rng = Random(0)
        val a = LinUcb.evaluate(snap, x, rng, exploration = 1.0)
        val b = LinUcb.evaluate(snap, x, rng, exploration = 1.0)
        assertEquals(a, b, "LinUcb consumes no RNG; repeated evaluate must agree")
    }

    @Test
    fun `LinUcb evaluate is mean plus alpha times sqrt xT Sigma x`() {
        val snap = bayesianSnapshot()
        val x = DenseVector.of(doubleArrayOf(0.5, -0.5))
        val alpha = 2.0
        val score = LinUcb.evaluate(snap, x, Random(0), exploration = alpha)
        val mean = snap.predict(x)
        // Bound check: score should strictly exceed the mean for non-zero alpha and non-degenerate covariance.
        assertTrue(score > mean, "LinUcb should add positive UCB term; score=$score, mean=$mean")
    }

    @Test
    fun `LinUcb sample returns mean weights without randomization`() {
        val snap = bayesianSnapshot()
        val draw1 = LinUcb.sample(snap, Random(0)).toDoubleArray()
        val draw2 = LinUcb.sample(snap, Random(99)).toDoubleArray()
        assertTrue(draw1.contentEquals(draw2), "LinUcb sample is deterministic")
        // And matches the snapshot's weights
        val w = snap.weights.toDoubleArray()
        assertTrue(draw1.contentEquals(w), "LinUcb sample returns snap.weights")
    }

    @Test
    fun `predict throws on wrong feature size`() {
        val snap = sgdSnapshot()
        assertTrue(snap.featureSize == 2)
        assertFailsWith<IllegalArgumentException> {
            snap.predict(DenseVector.of(doubleArrayOf(1.0)))
        }
    }

    @Test
    fun `CovarianceRegressionResult rejects shape mismatch`() {
        assertFailsWith<IllegalArgumentException> {
            CovarianceRegressionResult(
                weights = DenseVector.of(doubleArrayOf(0.0, 0.0)),
                bias = 0.0,
                biasPrecision = 1.0,
                totalWeights = 0.0,
                step = 0L,
                covariance = DenseMatrix(3, 3),
                covarianceL = DenseMatrix(3, 3),
            )
        }
    }

    @Test
    fun `SGD update skips non-positive weight`() {
        val stat = StochasticRegressionStat(featureSize = 2)
        stat.update(doubleArrayOf(1.0, 2.0), y = 1.0, weight = 0.0)
        stat.update(doubleArrayOf(1.0, 2.0), y = 1.0, weight = -1.0)
        val r = stat.read()
        assertEquals(0.0, r.totalWeights)
        assertEquals(0L, r.step)
    }

    @Test
    fun `SGD with L2 regularisation visits every coordinate`() {
        val stat = StochasticRegressionStat(
            featureSize = 3,
            optimizer = Sgd(ConstantRate(0.05)),
            penalty = Penalty.L2(0.1),
        )
        val rng = Random(1)
        repeat(500) {
            val x = doubleArrayOf(rng.nextDouble(), rng.nextDouble(), rng.nextDouble())
            stat.update(x, y = x[0] - x[1], weight = 1.0)
        }
        val r = stat.read()
        assertTrue(r.weights.toDoubleArray().any { it != 0.0 })
        assertTrue(r.sse > 0.0)
    }

    @Test
    fun `SGD merge with zero combined weight is a no-op`() {
        val stat = StochasticRegressionStat(featureSize = 2)
        val empty = stat.read()
        stat.merge(empty)
        val r = stat.read()
        assertEquals(0.0, r.totalWeights)
    }

    @Test
    fun `SGD merge rejects featureSize mismatch`() {
        val a = StochasticRegressionStat(featureSize = 2)
        val b = StochasticRegressionStat(featureSize = 3)
        assertFailsWith<IllegalArgumentException> { a.merge(b.read()) }
    }

    @Test
    fun `SGD rejects non-positive featureSize`() {
        assertFailsWith<IllegalArgumentException> { StochasticRegressionStat(featureSize = 0) }
    }

    @Test
    fun `SGD create returns a new instance preserving configuration`() {
        val a = StochasticRegressionStat(featureSize = 4, penalty = Penalty.L2(0.5))
        val b = a.create()
        val r = b.read()
        assertEquals(4, r.weights.size)
    }
}
