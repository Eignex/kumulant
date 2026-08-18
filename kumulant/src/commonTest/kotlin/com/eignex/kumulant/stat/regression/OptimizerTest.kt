package com.eignex.kumulant.stat.regression

import com.eignex.koblas.DenseVector
import com.eignex.kumulant.fitLine
import com.eignex.kumulant.schema.expr.Const
import com.eignex.kumulant.schema.optimizer.Adagrad
import com.eignex.kumulant.schema.optimizer.Adam
import com.eignex.kumulant.schema.optimizer.Rmsprop
import com.eignex.kumulant.schema.optimizer.Sgd
import com.eignex.kumulant.stat.regression.glm.ConstantRate
import com.eignex.kumulant.stat.regression.glm.Penalty
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class OptimizerTest {

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
    fun `Adam rejects a beta of one rather than learning nothing`() {
        // At beta == 1.0 the bias correction 1 - beta^t is exactly zero while the moment it corrects
        // is identically zero too, so every delta would be 0/0 and the weights would stay NaN for
        // the life of the stat. The bound is half-open to keep that unreachable.
        assertFailsWith<IllegalArgumentException> { AdamOptimizer(2, Const(0.1), beta1 = 1.0) }
        assertFailsWith<IllegalArgumentException> { AdamOptimizer(2, Const(0.1), beta2 = 1.0) }
    }

    @Test
    fun `Adam still accepts a beta of zero`() {
        // The other end stays closed: beta1 = 0 is plain SGD on the first moment, which is degenerate
        // but well defined, and the bias correction is 1 - 0^t = 1.
        AdamOptimizer(2, Const(0.1), beta1 = 0.0, beta2 = 0.0)
    }

    @Test
    fun `RMSProp rejects a rho of one rather than diverging`() {
        // rho == 1.0 freezes the squared-gradient EMA at zero, so the effective step becomes
        // lr / sqrt(epsilon) - four orders of magnitude larger than intended - on every update.
        assertFailsWith<IllegalArgumentException> { RmspropOptimizer(2, Const(0.1), rho = 1.0) }
    }

    @Test
    fun `Sgd rejects a non-positive feature size like its siblings`() {
        assertFailsWith<IllegalArgumentException> { SgdOptimizer(0, Const(0.1)) }
        assertFailsWith<IllegalArgumentException> { SgdOptimizer(-1, Const(0.1)) }
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
