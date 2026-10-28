package com.eignex.kumulant.stat.regression

import com.eignex.koblas.DenseVector
import com.eignex.koblas.SparseVector
import com.eignex.kumulant.schema.expr.ScalarExpr
import com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat
import com.eignex.kumulant.stat.regression.glm.ConstantRate
import com.eignex.kumulant.stat.regression.glm.DiagonalRegressionStat
import com.eignex.kumulant.stat.regression.glm.ExponentialDecay
import com.eignex.kumulant.stat.regression.glm.LinearRegressionResult
import com.eignex.kumulant.stat.regression.glm.StepDecay
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat
import kotlinx.serialization.json.Json
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class VectorSerializationTest {

    private val json = Json { encodeDefaults = false }

    @Test
    fun `sparse and dense updates produce the same fit`() {
        val truth = doubleArrayOf(1.5, 0.0, 0.0, -2.0, 0.0)
        val dense = DiagonalRegressionStat(featureSize = 5, priorPrecision = 0.01)
        val sparse = DiagonalRegressionStat(featureSize = 5, priorPrecision = 0.01)
        val rng = Random(13)
        repeat(20) {
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
    fun `LinearRegressionResult round-trips polymorphically through sealed root`() {
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
