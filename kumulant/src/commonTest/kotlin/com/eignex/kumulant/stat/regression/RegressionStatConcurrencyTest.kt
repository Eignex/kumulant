package com.eignex.kumulant.stat.regression

import com.eignex.koblas.F64DenseVector
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat
import com.eignex.kumulant.stat.regression.glm.DiagonalRegressionStat
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat
import com.eignex.kumulant.stat.regression.glm.UnivariateRegressionStat
import kotlin.test.Test
import kotlin.test.assertEquals

class RegressionStatConcurrencyTest {

    private val xs = doubleArrayOf(0.1, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0)
    private val ys = doubleArrayOf(0.0, 1.0, 1.5, 2.5, 3.0, 4.5, 5.5)

    private val mvX = arrayOf(
        doubleArrayOf(0.1, -0.2),
        doubleArrayOf(0.5, 0.3),
        doubleArrayOf(1.0, 0.1),
        doubleArrayOf(1.5, -0.5),
        doubleArrayOf(2.0, 0.0),
        doubleArrayOf(2.5, 0.7),
        doubleArrayOf(3.0, -0.3),
    )

    @Test
    fun `UnivariateRegressionStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = UnivariateRegressionStat(concurrency = mode)
            for (i in xs.indices) s.update(xs[i], ys[i])
            s.read(0L)
        }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.slope, r.slope, 1e-9, "Univariate slope mode=$mode")
            assertEquals(ref.intercept, r.intercept, 1e-9, "Univariate intercept mode=$mode")
        }
    }

    @Test
    fun `CovarianceStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = CovarianceStat(concurrency = mode)
            for (i in xs.indices) s.update(xs[i], ys[i])
            s.read(0L)
        }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.covariance, r.covariance, 1e-9, "Covariance mode=$mode")
            assertEquals(ref.correlation, r.correlation, 1e-9, "Correlation mode=$mode")
        }
    }

    @Test
    fun `StochasticRegressionStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = StochasticRegressionStat(featureSize = 2, concurrency = mode)
            for (i in mvX.indices) s.update(F64DenseVector.of(mvX[i]), ys[i])
            s.read(0L)
        }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.bias, r.bias, 1e-9, "Stochastic bias mode=$mode")
            for (j in 0 until 2) assertEquals(ref.weights[j], r.weights[j], 1e-9, "Stochastic w[$j] mode=$mode")
        }
    }

    @Test
    fun `DiagonalRegressionStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = DiagonalRegressionStat(featureSize = 2, concurrency = mode)
            for (i in mvX.indices) s.update(F64DenseVector.of(mvX[i]), ys[i])
            s.read(0L)
        }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.bias, r.bias, 1e-9, "Diagonal bias mode=$mode")
            for (j in 0 until 2) assertEquals(ref.weights[j], r.weights[j], 1e-9, "Diagonal w[$j] mode=$mode")
        }
    }

    @Test
    fun `BayesianRegressionStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = BayesianRegressionStat(featureSize = 2, concurrency = mode)
            for (i in mvX.indices) s.update(F64DenseVector.of(mvX[i]), ys[i])
            s.read(0L)
        }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.bias, r.bias, 1e-9, "Bayesian bias mode=$mode")
            for (j in 0 until 2) assertEquals(ref.weights[j], r.weights[j], 1e-9, "Bayesian w[$j] mode=$mode")
        }
    }
}
