package com.eignex.kumulant.stat.summary

import com.eignex.koblas.DenseVector
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class MeanEstimateTest {
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
