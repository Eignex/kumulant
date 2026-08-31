package com.eignex.kumulant.stat.regression

import com.eignex.koblas.core.F64DenseMatrix
import com.eignex.koblas.core.F64DenseVector
import com.eignex.koblas.core.F64SparseVector
import com.eignex.koblas.core.F64VectorLike
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class SoftmaxRegressionResultTest {

    private class StridedVector(private val backing: DoubleArray) : F64VectorLike {
        override val size: Int get() = backing.size / 2
        override fun get(i: Int): Double = backing[i * 2]
        override fun toDoubleArray(): DoubleArray = DoubleArray(size) { this[it] }
    }

    private fun result(weights: Array<DoubleArray>, biases: DoubleArray): SoftmaxRegressionResult =
        SoftmaxRegressionResult(
            featureSize = weights[0].size,
            numClasses = weights.size,
            weights = F64DenseMatrix.of(weights),
            biases = F64DenseVector.of(biases),
            totalWeights = 0.0,
            step = 0L,
            crossEntropy = 0.0,
        )

    @Test
    fun `logit computes bias plus weight dot x for the requested class`() {
        val r = result(
            weights = arrayOf(doubleArrayOf(1.0, 2.0), doubleArrayOf(-1.0, 0.5)),
            biases = doubleArrayOf(0.5, -0.25),
        )
        val x = F64DenseVector.of(doubleArrayOf(3.0, 4.0))
        assertEquals(0.5 + 1.0 * 3.0 + 2.0 * 4.0, r.logit(x, 0), 1e-12)
        assertEquals(-0.25 + -1.0 * 3.0 + 0.5 * 4.0, r.logit(x, 1), 1e-12)
    }

    @Test
    fun `logit rejects wrong feature size`() {
        val r = result(arrayOf(doubleArrayOf(1.0, 2.0)), doubleArrayOf(0.0))
        assertFailsWith<IllegalArgumentException> {
            r.logit(F64DenseVector.of(doubleArrayOf(1.0)), 0)
        }
    }

    @Test
    fun `sparse and generic inputs preserve logits probabilities and predictions`() {
        val r = result(
            weights = arrayOf(doubleArrayOf(1.0, -2.0, 0.5, 3.0), doubleArrayOf(-1.0, 1.0, 2.0, -0.5)),
            biases = doubleArrayOf(0.25, -0.75),
        )
        val dense = F64DenseVector.of(doubleArrayOf(2.0, 0.0, -1.0, 0.0))
        val sparse = F64SparseVector.of(4, intArrayOf(0, 2), doubleArrayOf(2.0, -1.0))
        val strided = StridedVector(doubleArrayOf(2.0, 9.0, 0.0, 9.0, -1.0, 9.0, 0.0, 9.0))

        val expected = r.probabilities(dense)
        for (x in listOf<F64VectorLike>(sparse, strided)) {
            val actual = r.probabilities(x)
            assertEquals(expected[0], actual[0], 1e-12)
            assertEquals(expected[1], actual[1], 1e-12)
            assertEquals(1.0, actual.sum(), 1e-12)
            assertEquals(r.predict(dense), r.predict(x))
            assertEquals(r.logit(dense, 0), r.logit(x, 0), 1e-12)
        }
    }

    @Test
    fun `init rejects mismatched weight shape`() {
        assertFailsWith<IllegalArgumentException> {
            SoftmaxRegressionResult(
                featureSize = 3,
                numClasses = 2,
                weights = F64DenseMatrix.of(arrayOf(doubleArrayOf(1.0, 2.0), doubleArrayOf(3.0, 4.0))),
                biases = F64DenseVector.of(doubleArrayOf(0.0, 0.0)),
                totalWeights = 0.0,
                step = 0L,
                crossEntropy = 0.0,
            )
        }
    }

    @Test
    fun `init rejects mismatched bias length`() {
        assertFailsWith<IllegalArgumentException> {
            SoftmaxRegressionResult(
                featureSize = 2,
                numClasses = 2,
                weights = F64DenseMatrix.of(arrayOf(doubleArrayOf(1.0, 2.0), doubleArrayOf(3.0, 4.0))),
                biases = F64DenseVector.of(doubleArrayOf(0.0)),
                totalWeights = 0.0,
                step = 0L,
                crossEntropy = 0.0,
            )
        }
    }
}
