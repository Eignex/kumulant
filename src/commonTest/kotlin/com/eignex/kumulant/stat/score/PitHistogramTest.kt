package com.eignex.kumulant.stat.score

import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private const val DELTA = 1e-12

class PitHistogramTest {

    @Test
    fun `uniform PIT input gives roughly equal bin weights`() {
        val numBins = 10
        val h = pitHistogram(numBins)
        // Spread N values evenly across [0, 1) so each bin gets exactly N/numBins.
        val perBin = 50
        val total = numBins * perBin
        for (i in 0 until total) {
            val pit = (i.toDouble() + 0.5) / total
            h.update(pit)
        }
        val res = h.read(0L)
        // numBins finite buckets, no under/overflow with values in (0,1).
        assertEquals(numBins, res.weights.size)
        for (w in res.weights) {
            assertEquals(perBin.toDouble(), w, DELTA)
        }
    }

    @Test
    fun `concentrated PIT input concentrates mass`() {
        val h = pitHistogram(4)
        repeat(100) { h.update(0.05) }  // bin 0
        repeat(10) { h.update(0.95) }   // bin 3
        val res = h.read(0L)
        var zeroBin = -1.0
        for (i in res.lowerBounds.indices) {
            if (abs(res.lowerBounds[i] - 0.0) < DELTA) { zeroBin = res.weights[i]; break }
        }
        assertTrue(zeroBin >= 100.0 - DELTA)
    }

    @Test
    fun `out of range pit values flow to underflow or overflow`() {
        val h = pitHistogram(4)
        h.update(-0.1)  // underflow
        h.update(1.5)   // overflow
        val res = h.read(0L)
        var underWeight = -1.0
        var overWeight = -1.0
        for (i in res.lowerBounds.indices) {
            if (!res.lowerBounds[i].isFinite() && res.lowerBounds[i] < 0) underWeight = res.weights[i]
            if (!res.upperBounds[i].isFinite() && res.upperBounds[i] > 0) overWeight = res.weights[i]
        }
        assertTrue(abs(underWeight - 1.0) < DELTA, "underflow row missing or wrong weight: $underWeight")
        assertTrue(abs(overWeight - 1.0) < DELTA, "overflow row missing or wrong weight: $overWeight")
    }
}
