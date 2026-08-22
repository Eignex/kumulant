package com.eignex.kumulant.stat.regression

import com.eignex.koblas.core.F64DenseMatrix
import com.eignex.koblas.core.F64DenseVector
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class SoftmaxRegressionResultTest {

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
