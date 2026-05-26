package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.schema.Adagrad
import com.eignex.kumulant.schema.Adam
import com.eignex.kumulant.schema.Rmsprop
import com.eignex.kumulant.schema.Sgd
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class OptimizerTest {

    private fun fitLine(
        stat: StochasticRegressionStat,
        truth: DoubleArray,
        intercept: Double,
        n: Int = 4000,
        seed: Long = 42L,
    ) {
        val rng = Random(seed)
        repeat(n) {
            val x = DoubleArray(truth.size) { rng.nextDouble() * 2.0 - 1.0 }
            var y = intercept
            for (i in truth.indices) y += truth[i] * x[i]
            y += rng.nextDouble() * 0.02 - 0.01
            stat.update(x, y, 1.0)
        }
    }

    private fun assertConverges(stat: StochasticRegressionStat, truth: DoubleArray, intercept: Double, tol: Double) {
        fitLine(stat, truth, intercept)
        val r = stat.read()
        for (i in truth.indices) {
            assertTrue(abs(r.weights[i] - truth[i]) < tol, "weight[$i]=${r.weights[i]} truth=${truth[i]}")
        }
        assertTrue(abs(r.bias - intercept) < tol, "bias=${r.bias} truth=$intercept")
    }

    @Test
    fun `Adam converges on a small regression`() {
        val stat = StochasticRegressionStat(
            featureSize = 3,
            optimizer = Adam(learningRate = ConstantRate(0.05)),
        )
        assertConverges(stat, doubleArrayOf(1.5, -2.0, 0.5), intercept = 0.3, tol = 0.15)
    }

    @Test
    fun `Adagrad converges on a small regression`() {
        val stat = StochasticRegressionStat(
            featureSize = 3,
            optimizer = Adagrad(learningRate = ConstantRate(0.3)),
        )
        assertConverges(stat, doubleArrayOf(1.5, -2.0, 0.5), intercept = 0.3, tol = 0.15)
    }

    @Test
    fun `RMSProp converges on a small regression`() {
        val stat = StochasticRegressionStat(
            featureSize = 3,
            optimizer = Rmsprop(learningRate = ConstantRate(0.02)),
        )
        assertConverges(stat, doubleArrayOf(1.5, -2.0, 0.5), intercept = 0.3, tol = 0.2)
    }

    @Test
    fun `Sgd remains the default optimizer`() {
        val stat = StochasticRegressionStat(
            featureSize = 2,
            optimizer = Sgd(ConstantRate(0.05)),
        )
        assertConverges(stat, doubleArrayOf(0.5, -1.0), intercept = 0.0, tol = 0.2)
    }

    @Test
    fun `softmax with Adam learns a separable mixture`() {
        val stat = SoftmaxRegressionStat(
            featureSize = 2,
            numClasses = 3,
            optimizer = Adam(learningRate = ConstantRate(0.05)),
        )
        val rng = Random(123L)
        val centers = arrayOf(
            doubleArrayOf(2.0, 0.0),
            doubleArrayOf(-2.0, 2.0),
            doubleArrayOf(-2.0, -2.0),
        )
        repeat(1500) {
            val c = rng.nextInt(3)
            val x = doubleArrayOf(
                centers[c][0] + rng.nextDouble() * 0.4 - 0.2,
                centers[c][1] + rng.nextDouble() * 0.4 - 0.2,
            )
            stat.update(x, c.toDouble(), 1.0)
        }
        val r = stat.read()
        var correct = 0
        repeat(300) {
            val c = rng.nextInt(3)
            val x = DenseVector.of(
                doubleArrayOf(
                    centers[c][0] + rng.nextDouble() * 0.4 - 0.2,
                    centers[c][1] + rng.nextDouble() * 0.4 - 0.2,
                ),
            )
            if (r.predict(x) == c) correct++
        }
        assertTrue(correct.toDouble() / 300.0 > 0.9, "accuracy=${correct / 300.0}")
    }

    @Test
    fun `Penalty requires Sgd optimizer`() {
        assertFailsWith<IllegalArgumentException> {
            StochasticRegressionStat(
                featureSize = 2,
                optimizer = Adam(),
                penalty = Penalty.L2(0.1),
            )
        }
    }
}
