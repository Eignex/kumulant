package com.eignex.kumulant.stat.summary

import com.eignex.koblas.DenseVector
import com.eignex.koblas.SparseVector
import com.eignex.koblas.StridedVector
import com.eignex.koblas.Vector
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class MeanEstimateTest {
    @Test
    fun `estimating with sparse and strided weights leaves caller storage unchanged`() {
        val values = DenseVector.of(doubleArrayOf(1.0, 3.0, 5.0))
        val expected = estimateMean(values, DenseVector.of(doubleArrayOf(1.0, 0.0, 2.0)))
        val variants = listOf(
            DenseVector.of(doubleArrayOf(1.0, 0.0, 2.0)),
            SparseVector.of(3, intArrayOf(0, 2), doubleArrayOf(1.0, 2.0)),
            StridedVector(doubleArrayOf(1.0, 9.0, 0.0, 9.0, 2.0, 9.0), 0, 3, 2),
        )

        for (weights in variants) {
            val result = estimateMean(values, weights)

            assertEquals(expected, result)
            assertTrue(weights.toDoubleArray().contentEquals(doubleArrayOf(1.0, 0.0, 2.0)))
        }
    }

    @Test
    fun `sparse and strided observations produce the same weighted estimate`() {
        val expected = estimateMean(
            DenseVector.of(doubleArrayOf(1.0, 0.0, 3.0)),
            DenseVector.of(doubleArrayOf(1.0, 0.0, 2.0)),
        )
        for (values in listOf<Vector>(
            SparseVector.of(3, intArrayOf(0, 2), doubleArrayOf(1.0, 3.0)),
            StridedVector(doubleArrayOf(9.0, 1.0, 9.0, 0.0, 9.0, 3.0), offset = 1, size = 3, stride = 2),
        )) {
            val weights = SparseVector.of(3, intArrayOf(0, 2), doubleArrayOf(1.0, 2.0))

            val actual = estimateMean(values, weights)

            assertEquals(expected, actual)
        }
    }

    @Test
    fun `normalizing weights leaves caller storage unchanged`() {
        val values = DenseVector.wrap(doubleArrayOf(1.0, 3.0))
        val weights = DenseVector.wrap(doubleArrayOf(2.0, 4.0))

        estimateMean(values, weights)

        assertTrue(values.values.contentEquals(doubleArrayOf(1.0, 3.0)))
        assertTrue(weights.values.contentEquals(doubleArrayOf(2.0, 4.0)))
    }

    @Test
    fun `nonfinite stored observations are rejected`() {
        for (value in listOf(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)) {
            val values = SparseVector.of(3, intArrayOf(1), doubleArrayOf(value))

            assertFailsWith<IllegalArgumentException> { estimateMean(values) }
        }
    }

    @Test
    fun `known noise propagates to a singleton mean without empirical dispersion`() {
        val result = estimateMean(
            DenseVector.of(doubleArrayOf(3.0)),
            noiseVariances = DenseVector.of(doubleArrayOf(4.0)),
        )
        assertEquals(3.0, result.mean)
        assertEquals(0.0, result.responseVariance)
        assertEquals(4.0, result.varianceOfMean)
    }

    @Test
    fun `rescaling importance weights preserves effective evidence and mean uncertainty`() {
        val values = DenseVector.of(doubleArrayOf(1.0, 3.0))
        val expected = estimateMean(values, DenseVector.of(doubleArrayOf(1.0, 2.0)))
        for (factor in listOf(Double.MIN_VALUE, 1e-300, 10.0, 1e300)) {
            val result = estimateMean(values, DenseVector.of(doubleArrayOf(factor, 2.0 * factor)))
            assertEquals(expected.effectiveSamples, result.effectiveSamples, 1e-12)
            assertEquals(expected.varianceOfMean!!, result.varianceOfMean!!, 1e-12)
        }
    }

    @Test
    fun `known noise and empirical response dispersion are not counted twice`() {
        val values = DenseVector.of(doubleArrayOf(1.0, 3.0))
        val result = estimateMean(values, noiseVariances = DenseVector.of(doubleArrayOf(4.0, 4.0)))
        assertEquals(2.0, result.responseVariance)
        assertEquals(2.0, result.varianceOfMean)
    }

    @Test
    fun `one unknown noise observation cannot provide a standard error`() {
        val result = estimateMean(DenseVector.of(doubleArrayOf(-2.0)))
        assertNull(result.varianceOfMean)
        assertEquals(-2.0, result.upperBound(0.0))
        assertTrue(result.upperBound(2.0).isInfinite())
    }

    @Test
    fun `paired differences retain correlation instead of summing marginal variances`() {
        val result = pairedMeanDifference(
            DenseVector.of(doubleArrayOf(101.0, 201.0, 301.0)),
            DenseVector.of(doubleArrayOf(100.0, 200.0, 300.0)),
        )
        assertEquals(1.0, result.mean)
        assertEquals(0.0, result.standardError)
    }
}
