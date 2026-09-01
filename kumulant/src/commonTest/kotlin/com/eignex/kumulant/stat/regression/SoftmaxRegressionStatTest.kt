package com.eignex.kumulant.stat.regression

import com.eignex.koblas.Workspace
import com.eignex.koblas.core.F64DenseMatrix
import com.eignex.koblas.core.F64DenseVector
import com.eignex.koblas.core.F64SparseVector
import com.eignex.koblas.core.F64StridedVectorView
import com.eignex.koblas.core.F64VectorLike
import com.eignex.kumulant.schema.optimizer.Sgd
import com.eignex.kumulant.stat.regression.glm.ConstantRate
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class SoftmaxRegressionStatTest {

    private class CustomVector(private val values: DoubleArray) : F64VectorLike {
        override val size: Int get() = values.size
        override fun get(i: Int): Double = values[i]
        override fun toDoubleArray(): DoubleArray = values.copyOf()
    }

    @Test
    fun `softmax probabilities sum to one and predict matches argmax`() {
        val r = SoftmaxRegressionResult(
            featureSize = 2,
            numClasses = 3,
            weights = F64DenseMatrix.of(
                arrayOf(
                    doubleArrayOf(1.0, 0.0),
                    doubleArrayOf(0.0, 1.0),
                    doubleArrayOf(-1.0, -1.0),
                ),
            ),
            biases = F64DenseVector.of(doubleArrayOf(0.0, 0.0, 0.0)),
            totalWeights = 0.0,
            step = 0L,
            crossEntropy = 0.0,
        )
        val p = r.probabilities(F64DenseVector.of(doubleArrayOf(2.0, 1.0)))
        val total = p.sum()
        assertEquals(1.0, total, 1e-9)
        // Class 0 has the largest logit (eta = 2), so argmax must be 0.
        assertEquals(0, r.predict(F64DenseVector.of(doubleArrayOf(2.0, 1.0))))
    }

    @Test
    fun `destination scoring matches allocating scoring and validates before mutation`() {
        val result = SoftmaxRegressionResult(
            featureSize = 2,
            numClasses = 3,
            weights = F64DenseMatrix.of(
                arrayOf(doubleArrayOf(1.0, 0.0), doubleArrayOf(0.0, 1.0), doubleArrayOf(-1.0, 1.0)),
            ),
            biases = F64DenseVector.of(doubleArrayOf(0.2, -0.1, 0.4)),
            totalWeights = 0.0,
            step = 0L,
            crossEntropy = 0.0,
        )
        val x = F64DenseVector.of(doubleArrayOf(2.0, -1.0))
        val workspace = Workspace().apply { reserve(3, 1) }
        val probabilities = DoubleArray(3)

        result.probabilitiesInto(x, probabilities)

        assertTrue(probabilities.contentEquals(result.probabilities(x)))
        assertEquals(result.predict(x), result.predict(x, workspace))
        val untouched = doubleArrayOf(7.0, 8.0)
        assertFailsWith<IllegalArgumentException> { result.probabilitiesInto(x, untouched) }
        assertTrue(untouched.contentEquals(doubleArrayOf(7.0, 8.0)))
    }

    @Test
    fun `destination scoring accepts dense sparse strided and custom vectors`() {
        val result = SoftmaxRegressionResult(
            featureSize = 2,
            numClasses = 3,
            weights = F64DenseMatrix.of(
                arrayOf(doubleArrayOf(1.0, -1.0), doubleArrayOf(2.0, 0.5), doubleArrayOf(-0.5, 1.5)),
            ),
            biases = F64DenseVector.of(doubleArrayOf(0.1, 0.2, -0.3)),
            totalWeights = 0.0,
            step = 0L,
            crossEntropy = 0.0,
        )
        val dense = F64DenseVector.of(doubleArrayOf(0.25, -0.5))
        val sparse = F64SparseVector.of(2, intArrayOf(0, 1), doubleArrayOf(0.25, -0.5))
        val strided = F64StridedVectorView(doubleArrayOf(0.25, 9.0, -0.5, 9.0), 0, 2, 2)
        val custom = CustomVector(doubleArrayOf(0.25, -0.5))
        val expected = result.probabilities(dense)

        for (x in listOf<F64VectorLike>(sparse, strided, custom)) {
            val actual = DoubleArray(3)
            result.probabilitiesInto(x, actual)
            assertTrue(actual.contentEquals(expected))
            assertEquals(result.predict(dense), result.predict(x, Workspace().apply { reserve(3, 1) }))
        }
    }

    @Test
    fun `failed workspace prediction releases its borrowed logits`() {
        val result = SoftmaxRegressionResult(
            featureSize = 2,
            numClasses = 2,
            weights = F64DenseMatrix.of(arrayOf(doubleArrayOf(1.0, 0.0), doubleArrayOf(0.0, 1.0))),
            biases = F64DenseVector.of(doubleArrayOf(0.0, 0.0)),
            totalWeights = 0.0,
            step = 0L,
            crossEntropy = 0.0,
        )
        val workspace = Workspace().apply { reserve(2, 1) }

        assertFailsWith<IllegalArgumentException> { result.predict(F64DenseVector.of(doubleArrayOf(1.0)), workspace) }
        assertEquals(0, result.predict(F64DenseVector.of(doubleArrayOf(1.0, 0.0)), workspace))
    }

    @Test
    fun `learns a separable three-class problem`() {
        val stat = SoftmaxRegressionStat(featureSize = 2, numClasses = 3, optimizer = Sgd(ConstantRate(0.2)))
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
            val x = F64DenseVector.of(
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
        val a = SoftmaxRegressionStat(featureSize = 2, numClasses = 2, optimizer = Sgd(ConstantRate(0.1)))
        val b = SoftmaxRegressionStat(featureSize = 2, numClasses = 2, optimizer = Sgd(ConstantRate(0.1)))
        repeat(100) { a.update(doubleArrayOf(1.0, 0.0), 1.0) }
        repeat(100) { b.update(doubleArrayOf(0.0, 1.0), 0.0) }
        a.merge(b.read())
        val r = a.read()
        assertEquals(200.0, r.totalWeights, 1e-9)
        assertTrue(abs(r.crossEntropy) > 0.0)
    }
}
