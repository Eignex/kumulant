package com.eignex.kumulant.stat.regression

import com.eignex.kumulant.math.DenseMatrix
import com.eignex.kumulant.math.DenseVector
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SoftmaxRegressionStatTest {

    @Test
    fun `softmax probabilities sum to one and predict matches argmax`() {
        val r = SoftmaxRegressionResult(
            featureSize = 2,
            numClasses = 3,
            weights = DenseMatrix.of(
                arrayOf(
                    doubleArrayOf(1.0, 0.0),
                    doubleArrayOf(0.0, 1.0),
                    doubleArrayOf(-1.0, -1.0),
                ),
            ),
            biases = DenseVector.of(doubleArrayOf(0.0, 0.0, 0.0)),
            totalWeights = 0.0,
            step = 0L,
            crossEntropy = 0.0,
        )
        val p = r.probabilities(DenseVector.of(doubleArrayOf(2.0, 1.0)))
        val total = p.sum()
        assertEquals(1.0, total, 1e-9)
        // Class 0 has the largest logit (eta = 2), so argmax must be 0.
        assertEquals(0, r.predict(DenseVector.of(doubleArrayOf(2.0, 1.0))))
    }

    @Test
    fun `learns a separable three-class problem`() {
        val stat = SoftmaxRegressionStat(featureSize = 2, numClasses = 3, learningRate = ConstantRate(0.2))
        val rng = Random(123L)
        // Three clusters around (1,0), (-1,1), (-1,-1).
        val centers = arrayOf(
            doubleArrayOf(2.0, 0.0),
            doubleArrayOf(-2.0, 2.0),
            doubleArrayOf(-2.0, -2.0),
        )
        repeat(3000) {
            val c = rng.nextInt(3)
            val x = doubleArrayOf(
                centers[c][0] + rng.nextDouble() * 0.4 - 0.2,
                centers[c][1] + rng.nextDouble() * 0.4 - 0.2,
            )
            stat.update(x, c.toDouble(), 1.0)
        }
        val r = stat.read()
        // Held-out accuracy on a fresh batch should be near perfect.
        var correct = 0
        val n = 600
        repeat(n) {
            val c = rng.nextInt(3)
            val x = DenseVector.of(
                doubleArrayOf(
                    centers[c][0] + rng.nextDouble() * 0.4 - 0.2,
                    centers[c][1] + rng.nextDouble() * 0.4 - 0.2,
                ),
            )
            if (r.predict(x) == c) correct++
        }
        assertTrue(correct.toDouble() / n > 0.95, "accuracy=${correct.toDouble() / n}")
    }

    @Test
    fun `update tracks total weights and step count`() {
        val stat = SoftmaxRegressionStat(featureSize = 2, numClasses = 2)
        stat.update(doubleArrayOf(0.1, 0.2), 1.0, weight = 1.0)
        stat.update(doubleArrayOf(0.3, 0.4), 0.0, weight = 2.0)
        val r = stat.read()
        assertEquals(3.0, r.totalWeights, 1e-9)
        assertEquals(2L, r.step)
    }

    @Test
    fun `out of range class labels are ignored`() {
        val stat = SoftmaxRegressionStat(featureSize = 1, numClasses = 2)
        stat.update(doubleArrayOf(1.0), 5.0, weight = 1.0)
        stat.update(doubleArrayOf(1.0), -1.0, weight = 1.0)
        assertEquals(0.0, stat.read().totalWeights, 1e-9)
    }

    @Test
    fun `reset clears parameters and counters`() {
        val stat = SoftmaxRegressionStat(featureSize = 2, numClasses = 3)
        repeat(50) { stat.update(doubleArrayOf(it.toDouble(), 1.0), (it % 3).toDouble()) }
        stat.reset()
        val r = stat.read()
        for (k in 0 until 3) for (i in 0 until 2) assertEquals(0.0, r.weights[k, i], 1e-12)
        for (k in 0 until 3) assertEquals(0.0, r.biases[k], 1e-12)
        assertEquals(0.0, r.totalWeights, 1e-12)
        assertEquals(0L, r.step)
    }

    @Test
    fun `merge blends weights sample-weighted`() {
        val a = SoftmaxRegressionStat(featureSize = 2, numClasses = 2, learningRate = ConstantRate(0.1))
        val b = SoftmaxRegressionStat(featureSize = 2, numClasses = 2, learningRate = ConstantRate(0.1))
        repeat(100) { a.update(doubleArrayOf(1.0, 0.0), 1.0) }
        repeat(100) { b.update(doubleArrayOf(0.0, 1.0), 0.0) }
        a.merge(b.read())
        val r = a.read()
        assertEquals(200.0, r.totalWeights, 1e-9)
        assertTrue(abs(r.crossEntropy) > 0.0)
    }
}
