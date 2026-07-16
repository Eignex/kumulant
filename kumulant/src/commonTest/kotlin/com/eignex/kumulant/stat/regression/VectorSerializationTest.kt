package com.eignex.kumulant.stat.regression

import com.eignex.koblas.DenseVector
import com.eignex.koblas.SparseVector
import com.eignex.koblas.VectorView
import com.eignex.kumulant.schema.expr.ScalarExpr
import com.eignex.kumulant.schema.optimizer.Sgd
import com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat
import com.eignex.kumulant.stat.regression.glm.ConstantRate
import com.eignex.kumulant.stat.regression.glm.CovarianceRegressionResult
import com.eignex.kumulant.stat.regression.glm.DiagonalRegressionStat
import com.eignex.kumulant.stat.regression.glm.ExponentialDecay
import com.eignex.kumulant.stat.regression.glm.FactorisedGaussian
import com.eignex.kumulant.stat.regression.glm.LinearRegressionResult
import com.eignex.kumulant.stat.regression.glm.MultivariateGaussian
import com.eignex.kumulant.stat.regression.glm.PointPosterior
import com.eignex.kumulant.stat.regression.glm.StepDecay
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat
import kotlinx.serialization.json.Json
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * End-to-end checks for the sealed VectorView abstraction: sparse inputs flow through
 * regression-stat updates without materialisation, and snapshots round-trip through
 * kotlinx.serialization preserving the concrete dense/sparse subtype.
 */
private inline fun mean(n: Int, draw: () -> Double): Double {
    var s = 0.0
    repeat(n) { s += draw() }
    return s / n
}

class VectorSerializationTest {

    private val json = Json { encodeDefaults = false }

    @Test
    fun `sparse and dense inputs converge to the same fit`() {
        val truth = doubleArrayOf(1.5, 0.0, 0.0, -2.0, 0.0)
        val dense = DiagonalRegressionStat(featureSize = 5, priorPrecision = 0.01)
        val sparse = DiagonalRegressionStat(featureSize = 5, priorPrecision = 0.01)
        val rng = Random(13)
        // Each observation activates a handful of features -> naturally sparse.
        repeat(3000) {
            val active = listOf(0, 3, rng.nextInt(5))
            val xArr = DoubleArray(5).also { arr -> for (i in active.distinct()) arr[i] = rng.nextDouble() * 2 - 1 }
            var y = 0.0
            for (i in 0 until 5) y += truth[i] * xArr[i]
            y += rng.nextDouble() * 0.02 - 0.01
            dense.update(DenseVector.of(xArr), y, 1.0)
            val nz = (0 until 5).filter { idx -> xArr[idx] != 0.0 }
            val xs = SparseVector.of(5, nz.toIntArray(), nz.map { idx -> xArr[idx] }.toDoubleArray())
            sparse.update(xs, y, 1.0)
        }
        val rd = dense.read()
        val rs = sparse.read()
        for (i in 0 until 5) {
            assertTrue(
                abs(rd.weights[i] - rs.weights[i]) < 1e-9,
                "dense and sparse paths diverge at i=$i: ${rd.weights[i]} vs ${rs.weights[i]}",
            )
        }
    }

    @Test
    fun `DenseVector round-trips through JSON`() {
        val v: VectorView = DenseVector.of(doubleArrayOf(1.0, -2.5, 3.14, 0.0))
        val wire = json.encodeToString(VectorView.serializer(), v)
        val decoded = json.decodeFromString(VectorView.serializer(), wire)
        assertTrue(decoded is DenseVector)
        assertEquals(v, decoded)
    }

    @Test
    fun `SparseVector round-trips through JSON`() {
        val v: VectorView = SparseVector.of(10, intArrayOf(2, 5, 9), doubleArrayOf(1.0, -2.0, 0.5))
        val wire = json.encodeToString(VectorView.serializer(), v)
        val decoded = json.decodeFromString(VectorView.serializer(), wire)
        assertTrue(decoded is SparseVector)
        assertEquals(v, decoded)
    }

    @Test
    fun `learning rate schedules round-trip through JSON`() {
        val constant = ConstantRate(0.05)
        val step = StepDecay(0.01, 1e-3)
        val expDecay = ExponentialDecay(0.01, 1e-5)
        for (e in listOf(constant, step, expDecay)) {
            val wire = json.encodeToString(ScalarExpr.serializer(), e)
            val decoded = json.decodeFromString(ScalarExpr.serializer(), wire)
            for (s in longArrayOf(0L, 100L, 10_000L)) {
                assertEquals(
                    e.eval(s.toDouble()),
                    decoded.eval(s.toDouble()),
                    "schedule diverged at step=$s: $e",
                )
            }
        }
    }

    @Test
    fun `CovarianceRegressionResult round-trips through JSON`() {
        val stat = BayesianRegressionStat(featureSize = 3, priorVariance = 1.0)
        val truth = doubleArrayOf(0.5, -1.0, 0.7)
        val rng = Random(99)
        repeat(500) {
            val x = DoubleArray(3) { rng.nextDouble() * 2 - 1 }
            var y = 0.0
            for (i in 0 until 3) y += truth[i] * x[i]
            stat.update(x, y, 1.0)
        }
        val before = stat.read()
        val wire = json.encodeToString(CovarianceRegressionResult.serializer(), before)
        val after = json.decodeFromString(CovarianceRegressionResult.serializer(), wire)
        assertEquals(before, after)
    }

    @Test
    fun `LinearPosterior evaluate matches mean plus calibrated Gaussian noise`() {
        // Average evaluate(x) over ~2k draws and check the sample mean lands near
        // predict(x) for each posterior shape. Distributional correctness of the
        // underlying samplers is exercised in DistributionsTest and MathTest.
        val rng = Random(2026)
        val truth = doubleArrayOf(0.7, -0.3, 1.5)

        val sgd = StochasticRegressionStat(featureSize = 3, optimizer = Sgd(ConstantRate(0.05)))
        val diag = DiagonalRegressionStat(featureSize = 3, priorPrecision = 0.01)
        val bayes = BayesianRegressionStat(featureSize = 3, priorVariance = 1.0)
        repeat(2000) {
            val xArr = DoubleArray(3) { rng.nextDouble() * 2 - 1 }
            var y = 0.0
            for (i in 0 until 3) y += truth[i] * xArr[i]
            sgd.update(xArr, y, 1.0)
            diag.update(xArr, y, 1.0)
            bayes.update(xArr, y, 1.0)
        }
        val queryX = DenseVector.of(doubleArrayOf(0.4, -0.2, 0.6))
        val analyticMean = 0.7 * 0.4 + -0.3 * -0.2 + 1.5 * 0.6

        for ((label, m) in listOf(
            "PointPosterior" to mean(2000) {
                PointPosterior.evaluate(sgd.read(), queryX, rng, exploration = 0.1)
            },
            "FactorisedGaussian" to mean(2000) {
                FactorisedGaussian.evaluate(diag.read(), queryX, rng, exploration = 1.0)
            },
            "MultivariateGaussian" to mean(2000) {
                MultivariateGaussian.evaluate(bayes.read(), queryX, rng, exploration = 1.0)
            },
        )) {
            assertTrue(abs(m - analyticMean) < 0.2, "$label evaluate mean=$m far from $analyticMean")
        }
    }

    @Test
    fun `LinearRegressionResult round-trips polymorphically through sealed root`() {
        // The sealed root carries the polymorphic discriminator so combo's
        // LinearLearnerData(state: LinearRegressionResult) can wire-encode without
        // knowing which concrete subtype a particular bandit produced.
        val sgd = StochasticRegressionStat(featureSize = 2).also { it.update(doubleArrayOf(1.0, 2.0), 0.5) }
        val diag = DiagonalRegressionStat(featureSize = 2, priorPrecision = 0.1)
            .also { it.update(doubleArrayOf(1.0, 2.0), 0.5) }
        val bayes = BayesianRegressionStat(featureSize = 2, priorVariance = 0.5)
            .also { it.update(doubleArrayOf(1.0, 2.0), 0.5) }

        for (snap in listOf<LinearRegressionResult>(sgd.read(), diag.read(), bayes.read())) {
            val wire = json.encodeToString(LinearRegressionResult.serializer(), snap)
            val decoded = json.decodeFromString(LinearRegressionResult.serializer(), wire)
            assertEquals(snap, decoded, "round-trip failed for ${snap::class.simpleName}")
            assertEquals(snap::class, decoded::class, "subtype changed on round-trip")
        }
    }
}
