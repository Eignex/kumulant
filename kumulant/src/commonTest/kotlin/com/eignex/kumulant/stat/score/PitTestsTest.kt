package com.eignex.kumulant.stat.score

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.stat.quantile.SparseHistogramResult
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PitTestsTest {

    @Test
    fun `uniform pit gives near zero chi squared and ks`() {
        val numBins = 10
        val h = pitHistogram(numBins)
        val perBin = 50
        val total = numBins * perBin
        for (i in 0 until total) {
            h.update((i + 0.5) / total)
        }
        val res = h.read(0L)
        assertEquals(0.0, res.pitChiSquared(numBins), DELTA)
        assertTrue(res.pitKsDistance(numBins) < 0.05, "KS=${res.pitKsDistance(numBins)} larger than expected")
    }

    @Test
    fun `concentrated pit gives high chi squared and ks`() {
        val numBins = 4
        val h = pitHistogram(numBins)
        repeat(100) { h.update(0.05) }
        val res = h.read(0L)
        assertEquals(300.0, res.pitChiSquared(numBins), DELTA)
        assertEquals(0.75, res.pitKsDistance(numBins), DELTA)
    }

    @Test
    fun `empty histogram gives zero`() {
        val empty = SparseHistogramResult(DoubleArray(0), DoubleArray(0), DoubleArray(0))
        assertEquals(0.0, empty.pitChiSquared(numBins = 10), DELTA)
        assertEquals(0.0, empty.pitKsDistance(numBins = 10), DELTA)
    }

    @Test
    fun `under and overflow rows are excluded`() {
        val res = SparseHistogramResult(
            lowerBounds = doubleArrayOf(Double.NEGATIVE_INFINITY, 0.0, 1.0),
            upperBounds = doubleArrayOf(0.0, 1.0, Double.POSITIVE_INFINITY),
            weights = doubleArrayOf(99.0, 10.0, 99.0),
        )
        assertEquals(0.0, res.pitChiSquared(numBins = 1), DELTA)
        assertEquals(0.0, res.pitKsDistance(numBins = 1), DELTA)
    }

    @Test
    fun `chi squared counts the top edge row when its bin is otherwise empty`() {
        val h = pitHistogram(2)
        h.update(0.25)
        h.update(1.0)
        val res = h.read(0L)
        assertEquals(0.0, res.pitChiSquared(2), DELTA)
    }

    @Test
    fun `zero pit values contribute to the bottom bin`() {
        val histogram = pitHistogram(4)
        repeat(100) { histogram.update(0.0) }
        val result = histogram.read(0L)

        for ((score, expected) in listOf(
            SparseHistogramResult::pitChiSquared to 300.0,
            SparseHistogramResult::pitKsDistance to 0.75,
        )) {
            assertEquals(expected, score(result, 4), DELTA)
        }
    }

    @Test
    fun `nonfinite rows outside the bottom edge do not affect uniformity`() {
        val result = SparseHistogramResult(
            lowerBounds = doubleArrayOf(Double.NEGATIVE_INFINITY, 0.0, 0.5, 1.0, Double.NaN),
            upperBounds = doubleArrayOf(-0.1, 0.5, 1.0, Double.POSITIVE_INFINITY, Double.NaN),
            weights = doubleArrayOf(99.0, 10.0, 10.0, 99.0, 99.0),
        )

        for (score in listOf(SparseHistogramResult::pitChiSquared, SparseHistogramResult::pitKsDistance)) {
            assertEquals(0.0, score(result, 2), DELTA)
        }
    }
}
