package com.eignex.kumulant.stat.summary

import com.eignex.koblas.DenseMatrix
import com.eignex.koblas.DenseVector
import com.eignex.kumulant.math.factorCholesky
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class CorrelatedMeanEstimateTest {
    @Test
    fun `diagonal covariance gives the inverse variance weighted mean`() {
        val covariance = DenseMatrix.ofRows(arrayOf(doubleArrayOf(1.0, 0.0), doubleArrayOf(0.0, 4.0)))
        val estimate = estimateCorrelatedMean(DenseVector.of(doubleArrayOf(2.0, 7.0)), covariance.factorCholesky())
        assertEquals(3.0, estimate.mean, 1e-12)
        assertEquals(0.8, estimate.varianceOfMean, 1e-12)
    }

    @Test
    fun `correlation permits negative coefficients without discarding information`() {
        val covariance = DenseMatrix.ofRows(arrayOf(doubleArrayOf(1.0, 1.5), doubleArrayOf(1.5, 4.0)))
        val factor = covariance.factorCholesky()
        val values = DenseVector.of(doubleArrayOf(2.0, 6.0))
        val estimate = estimateCorrelatedMean(values, factor)
        assertEquals(1.0, estimate.mean, 1e-12)
        assertEquals(0.875, estimate.varianceOfMean, 1e-12)
        assertEquals(listOf(2.0, 6.0), values.toDoubleArray().toList())
        assertEquals(estimate, estimateCorrelatedMean(values, factor))
    }

    @Test
    fun `common offsets and covariance scale preserve location equivariance`() {
        val covariance = DenseMatrix.ofRows(arrayOf(doubleArrayOf(9.0, 0.0), doubleArrayOf(0.0, 36.0)))
        val estimate = estimateCorrelatedMean(
            DenseVector.of(doubleArrayOf(1e12 + 2.0, 1e12 + 7.0)),
            covariance.factorCholesky(),
        )
        assertEquals(1e12 + 3.0, estimate.mean)
        assertEquals(7.2, estimate.varianceOfMean, 1e-12)
    }

    @Test
    fun `nearly singular correlation preserves common location precision`() {
        val correlation = 1.0 - 1e-12
        val covariance = DenseMatrix.ofRows(
            arrayOf(
            doubleArrayOf(1.0, correlation),
            doubleArrayOf(correlation, 1.0),
        )
        )
        val estimate = estimateCorrelatedMean(
            DenseVector.of(doubleArrayOf(1.0, 3.0)),
            covariance.factorCholesky(),
        )
        assertEquals(2.0, estimate.mean, 1e-10)
        assertEquals((1.0 + correlation) / 2, estimate.varianceOfMean, 1e-12)
    }

    @Test
    fun `invalid evidence is rejected`() {
        val factor = DenseMatrix.ofRows(arrayOf(doubleArrayOf(1.0))).factorCholesky()
        for (values in listOf(doubleArrayOf(), doubleArrayOf(1.0, 2.0), doubleArrayOf(Double.NaN))) {
            assertFailsWith<IllegalArgumentException> { estimateCorrelatedMean(DenseVector.of(values), factor) }
        }
    }
}
