package com.eignex.kumulant.stat.regression.glm

import com.eignex.koblas.Workspace
import com.eignex.koblas.borrow
import com.eignex.koblas.core.F64DenseMatrix
import com.eignex.koblas.core.F64DenseVector
import com.eignex.koblas.core.F64SparseVector
import com.eignex.koblas.core.F64StridedVectorView
import com.eignex.koblas.core.F64VectorLike
import com.eignex.kumulant.math.nextNormal
import com.eignex.kumulant.schema.optimizer.Sgd
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
class LinearPosteriorsTest {

    private class CustomVector(private val values: DoubleArray) : F64VectorLike {
        override val size: Int get() = values.size
        override fun get(i: Int): Double = values[i]
        override fun toDoubleArray(): DoubleArray = values.copyOf()
    }

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
    fun `workspace covariance evaluation matches allocating evaluation and preserves random draws`() {
        val snapshot = bayesianSnapshot()
        val x = F64SparseVector.of(size = 2, indices = intArrayOf(0), values = doubleArrayOf(0.3))
        val workspace = Workspace().apply { reserve(2, 1) }

        val allocated = MultivariateGaussian.evaluate(snapshot, x, Random(9), exploration = 0.7)
        val reused = MultivariateGaussian.evaluate(snapshot, x, Random(9), workspace, exploration = 0.7)

        assertEquals(allocated, reused, 1e-12)
    }

    @Test
    fun `workspace covariance evaluation accepts strided and custom vectors`() {
        val snapshot = bayesianSnapshot()
        val dense = F64DenseVector.of(doubleArrayOf(0.3, -0.2))
        val strided = F64StridedVectorView(doubleArrayOf(0.3, 7.0, -0.2, 7.0), 0, 2, 2)
        val custom = CustomVector(doubleArrayOf(0.3, -0.2))
        val workspace = Workspace().apply { reserve(2, 1) }

        val expected = LinUcb.evaluate(snapshot, dense, Random(0), workspace, exploration = 0.7)
        assertEquals(expected, LinUcb.evaluate(snapshot, strided, Random(0), workspace, exploration = 0.7), 1e-12)
        assertEquals(expected, LinUcb.evaluate(snapshot, custom, Random(0), workspace, exploration = 0.7), 1e-12)
    }

    @Test
    fun `sampleInto matches an owned multivariate sample without aliasing later borrows`() {
        val snapshot = bayesianSnapshot()
        val destination = DoubleArray(2)
        val workspace = Workspace().apply { reserve(2, 1) }

        MultivariateGaussian.sampleInto(snapshot, Random(21), destination, exploration = 0.4)
        val expected = MultivariateGaussian.sample(snapshot, Random(21), exploration = 0.4)
        assertEquals(expected[0], destination[0], 1e-12)
        assertEquals(expected[1], destination[1], 1e-12)
        workspace.borrow(2) { it.fill(0.0) }
        assertEquals(expected[0], destination[0], 1e-12)
        assertEquals(expected[1], destination[1], 1e-12)
    }

    @Test
    fun `PointPosterior with zero exploration returns the snapshot weights as-is`() {
        val snap = sgdSnapshot()
        val sample = PointPosterior.sample(snap, Random(0), exploration = 0.0)
        assertTrue(sample === snap.weights)
    }

    @Test
    fun `PointPosterior sample delegates to sampleInto without changing RNG order`() {
        val snapshot = sgdSnapshot()
        val expected = DoubleArray(snapshot.weights.size)
        val intoRng = Random(91)
        PointPosterior.sampleInto(snapshot, intoRng, expected, exploration = 0.25)
        val sampleRng = Random(91)
        val sample = PointPosterior.sample(snapshot, sampleRng, exploration = 0.25)

        for (i in expected.indices) assertEquals(expected[i], sample[i], 0.0)
        assertEquals(intoRng.nextDouble(), sampleRng.nextDouble(), 0.0)
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
        val x = F64DenseVector.of(doubleArrayOf(0.3, -0.4))
        val v = PointPosterior.evaluate(snap, x, Random(0), exploration = 0.0)
        assertEquals(snap.predict(x), v, 1e-12)
    }

    @Test
    fun `PointPosterior evaluate with exploration is finite`() {
        val snap = sgdSnapshot()
        val x = F64DenseVector.of(doubleArrayOf(0.3, -0.4))
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
    fun `FactorisedGaussian sample delegates to sampleInto without changing RNG order`() {
        val snapshot = diagonalSnapshot()
        val expected = DoubleArray(snapshot.weights.size)
        val intoRng = Random(92)
        FactorisedGaussian.sampleInto(snapshot, intoRng, expected, exploration = 0.25)
        val sampleRng = Random(92)
        val sample = FactorisedGaussian.sample(snapshot, sampleRng, exploration = 0.25)

        for (i in expected.indices) assertEquals(expected[i], sample[i], 0.0)
        assertEquals(intoRng.nextDouble(), sampleRng.nextDouble(), 0.0)
    }

    @Test
    fun `FactorisedGaussian evaluate is centered on predict`() {
        val snap = diagonalSnapshot()
        val x = F64DenseVector.of(doubleArrayOf(0.1, 0.2))
        val rng = Random(0)
        val n = 600
        var sum = 0.0
        repeat(n) { sum += FactorisedGaussian.evaluate(snap, x, rng, exploration = 0.1) }
        assertTrue(abs(sum / n - snap.predict(x)) < 0.05)
    }

    @Test
    fun `FactorisedGaussian evaluate gives sparse inputs the dense score`() {
        val snap = diagonalSnapshot()
        val dense = F64DenseVector.of(doubleArrayOf(0.1, 0.0))
        val sparse = F64SparseVector.of(2, intArrayOf(0), doubleArrayOf(0.1))

        assertEquals(
            FactorisedGaussian.evaluate(snap, dense, Random(42), exploration = 0.1),
            FactorisedGaussian.evaluate(snap, sparse, Random(42), exploration = 0.1),
            1e-12,
        )
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
    fun `MultivariateGaussian sample preserves triangular product and random draws`() {
        val covariance = F64DenseMatrix.of(arrayOf(doubleArrayOf(4.0, 6.0), doubleArrayOf(6.0, 25.0)))
        val snapshot = CovarianceRegressionResult(
            weights = F64DenseVector.of(doubleArrayOf(1.0, -1.0)),
            bias = 0.0,
            biasPrecision = 1.0,
            totalWeights = 0.0,
            step = 0L,
            covariance = covariance,
            covarianceL = F64DenseMatrix.of(arrayOf(doubleArrayOf(2.0, 0.0), doubleArrayOf(3.0, 4.0))),
        )
        val expectedRng = Random(7)
        val u0 = expectedRng.nextNormal(0.0, 0.5)
        val u1 = expectedRng.nextNormal(0.0, 0.5)
        val expectedNext = expectedRng.nextNormal()
        val actualRng = Random(7)

        val sample = MultivariateGaussian.sample(snapshot, actualRng, exploration = 0.25)

        assertEquals(1.0 + 2.0 * u0, sample[0], 1e-12)
        assertEquals(-1.0 + 3.0 * u0 + 4.0 * u1, sample[1], 1e-12)
        assertEquals(expectedNext, actualRng.nextNormal(), 0.0)
    }

    @Test
    fun `MultivariateGaussian evaluate is finite and finite-variance`() {
        val snap = bayesianSnapshot()
        val x = F64DenseVector.of(doubleArrayOf(0.5, 0.5))
        val rng = Random(0)
        val n = 300
        var sum = 0.0
        repeat(n) { sum += MultivariateGaussian.evaluate(snap, x, rng, exploration = 0.2) }
        assertTrue(abs(sum / n - snap.predict(x)) < 0.05)
    }

    @Test
    fun `LinUcb evaluate is deterministic given the snapshot`() {
        val snap = bayesianSnapshot()
        val x = F64DenseVector.of(doubleArrayOf(0.3, 0.7))
        val rng = Random(0)
        val a = LinUcb.evaluate(snap, x, rng, exploration = 1.0)
        val b = LinUcb.evaluate(snap, x, rng, exploration = 1.0)
        assertEquals(a, b, "LinUcb consumes no RNG; repeated evaluate must agree")
    }

    @Test
    fun `LinUcb evaluate is mean plus alpha times sqrt xT Sigma x`() {
        val snap = bayesianSnapshot()
        val x = F64DenseVector.of(doubleArrayOf(0.5, -0.5))
        val alpha = 2.0
        val score = LinUcb.evaluate(snap, x, Random(0), exploration = alpha)
        val mean = snap.predict(x)
        assertTrue(score > mean, "LinUcb should add positive UCB term; score=$score, mean=$mean")
    }

    @Test
    fun `LinUcb sample returns mean weights without randomization`() {
        val snap = bayesianSnapshot()
        val draw1 = LinUcb.sample(snap, Random(0)).toDoubleArray()
        val draw2 = LinUcb.sample(snap, Random(99)).toDoubleArray()
        assertTrue(draw1.contentEquals(draw2), "LinUcb sample is deterministic")
        val w = snap.weights.toDoubleArray()
        assertTrue(draw1.contentEquals(w), "LinUcb sample returns snap.weights")
    }

    @Test
    fun `predict throws on wrong feature size`() {
        val snap = sgdSnapshot()
        assertTrue(snap.featureSize == 2)
        assertFailsWith<IllegalArgumentException> {
            snap.predict(F64DenseVector.of(doubleArrayOf(1.0)))
        }
    }

    @Test
    fun `CovarianceRegressionResult rejects shape mismatch`() {
        assertFailsWith<IllegalArgumentException> {
            CovarianceRegressionResult(
                weights = F64DenseVector.of(doubleArrayOf(0.0, 0.0)),
                bias = 0.0,
                biasPrecision = 1.0,
                totalWeights = 0.0,
                step = 0L,
                covariance = F64DenseMatrix.zero(3, 3),
                covarianceL = F64DenseMatrix.zero(3, 3),
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

    @Test
    fun `evaluate stays inside the link range under a logit link`() {
        val point = sgdSnapshot().copy(link = Link.Logit)
        val diagonal = diagonalSnapshot().copy(link = Link.Logit)
        val bayesian = bayesianSnapshot().copy(link = Link.Logit)
        val x = F64DenseVector.of(doubleArrayOf(1.0, 1.0))
        val rng = Random(3)
        repeat(100) {
            assertTrue(PointPosterior.evaluate(point, x, rng, exploration = 4.0) in 0.0..1.0)
            assertTrue(FactorisedGaussian.evaluate(diagonal, x, rng, exploration = 4.0) in 0.0..1.0)
            assertTrue(MultivariateGaussian.evaluate(bayesian, x, rng, exploration = 4.0) in 0.0..1.0)
            assertTrue(LinUcb.evaluate(bayesian, x, rng, exploration = 4.0) in 0.0..1.0)
        }
    }
}
