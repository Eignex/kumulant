package com.eignex.kumulant.stat.quantile

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private const val DELTA = 1e-9

class HistogramResultsTest {

    private fun sketch(
        zeroCount: Double = 0.0,
        positiveBins: Map<Int, Double> = emptyMap(),
        negativeBins: Map<Int, Double> = emptyMap(),
    ): SketchResult = SketchResult(
        probabilities = doubleArrayOf(),
        quantiles = doubleArrayOf(),
        gamma = 1.02,
        totalWeights = zeroCount + positiveBins.values.sum() + negativeBins.values.sum(),
        zeroCount = zeroCount,
        positiveBins = positiveBins,
        negativeBins = negativeBins,
    )

    @Test
    fun `empty sketch produces empty sparse histogram`() {
        val hist = sketch().toSparseHistogram()
        assertEquals(0, hist.lowerBounds.size)
        assertEquals(0, hist.upperBounds.size)
        assertEquals(0, hist.weights.size)
    }

    @Test
    fun `zero-only sketch has a single zero-width bucket at the origin`() {
        val hist = sketch(zeroCount = 5.0).toSparseHistogram()
        assertEquals(1, hist.weights.size)
        assertEquals(0.0, hist.lowerBounds[0], DELTA)
        assertEquals(0.0, hist.upperBounds[0], DELTA)
        assertEquals(5.0, hist.weights[0], DELTA)
    }

    @Test
    fun `single positive bucket expands to lower and upper bounds around it`() {
        val hist = sketch(positiveBins = mapOf(1 to 3.0)).toSparseHistogram()
        assertEquals(1, hist.weights.size)
        assertTrue(hist.lowerBounds[0] < hist.upperBounds[0])
        assertEquals(3.0, hist.weights[0], DELTA)
    }

    @Test
    fun `single negative bucket has negative bounds`() {
        val hist = sketch(negativeBins = mapOf(1 to 4.0)).toSparseHistogram()
        assertEquals(1, hist.weights.size)
        assertTrue(hist.upperBounds[0] < 0.0)
        assertTrue(hist.lowerBounds[0] < hist.upperBounds[0])
        assertEquals(4.0, hist.weights[0], DELTA)
    }

    @Test
    fun `mixed sketch orders negative then zero then positive buckets`() {
        val hist = sketch(
            zeroCount = 2.0,
            positiveBins = mapOf(1 to 1.0, 2 to 1.0),
            negativeBins = mapOf(1 to 1.0, 2 to 1.0),
        ).toSparseHistogram()

        assertEquals(5, hist.weights.size)
        for (i in 1 until hist.lowerBounds.size) {
            assertTrue(
                hist.lowerBounds[i] >= hist.lowerBounds[i - 1],
                "bounds not sorted: ${hist.lowerBounds.toList()}"
            )
        }
        assertEquals(6.0, hist.weights.sum(), DELTA)
    }
}
