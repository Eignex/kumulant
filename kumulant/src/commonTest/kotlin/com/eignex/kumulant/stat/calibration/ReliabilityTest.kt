package com.eignex.kumulant.stat.calibration

import com.eignex.kumulant.DELTA
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ReliabilityTest {

    @Test
    fun `bins by predicted probability`() {
        val r = ReliabilityStat(numBins = 4).apply {
            // bin width 0.25
            update(x = 0.1, y = 0.0)
            update(x = 0.3, y = 1.0)
            update(x = 0.6, y = 1.0)
            update(x = 0.9, y = 1.0)
        }
        val res = r.read(0L)
        assertEquals(1.0, res.totalWeights[0], DELTA)
        assertEquals(1.0, res.totalWeights[1], DELTA)
        assertEquals(1.0, res.totalWeights[2], DELTA)
        assertEquals(1.0, res.totalWeights[3], DELTA)
        assertEquals(0.1, res.meanProbability[0], DELTA)
        assertEquals(0.3, res.meanProbability[1], DELTA)
        assertEquals(0.6, res.meanProbability[2], DELTA)
        assertEquals(0.9, res.meanProbability[3], DELTA)
        assertEquals(0.0, res.outcomeRate[0], DELTA)
        assertEquals(1.0, res.outcomeRate[1], DELTA)
        assertEquals(1.0, res.outcomeRate[2], DELTA)
        assertEquals(1.0, res.outcomeRate[3], DELTA)
    }

    @Test
    fun `empty bins read NaN`() {
        val r = ReliabilityStat(numBins = 3)
        val res = r.read(0L)
        for (i in 0 until 3) {
            assertTrue(res.meanProbability[i].isNaN())
            assertTrue(res.outcomeRate[i].isNaN())
        }
    }

    @Test
    fun `out of range probability clamps to edge bin`() {
        val r = ReliabilityStat(2).apply {
            update(-0.5, 0.0)
            update(1.5, 1.0)
        }
        val res = r.read(0L)
        assertEquals(1.0, res.totalWeights[0], DELTA)
        assertEquals(1.0, res.totalWeights[1], DELTA)
        // Clamping must apply to the accumulated probability too, otherwise meanProbability
        // can drift outside [0, 1] and corrupt ECE.
        assertEquals(0.0, res.meanProbability[0], DELTA)
        assertEquals(1.0, res.meanProbability[1], DELTA)
        assertTrue(res.expectedCalibrationError() in 0.0..1.0)
    }

    @Test
    fun `perfectly calibrated stream gives ECE 0`() {
        val r = ReliabilityStat(2).apply {
            update(0.2, 0.0)
            update(0.2, 0.0)
            update(0.2, 1.0)
            update(0.2, 1.0)
            update(0.8, 1.0)
            update(0.8, 1.0)
            update(0.8, 1.0)
            update(0.8, 0.0)
        }
        val res = r.read(0L)
        // Bin 0: meanP=0.2, rate=0.5, gap=0.3. Bin 1: meanP=0.8, rate=0.75, gap=0.05.
        // ECE = 0.5*0.3 + 0.5*0.05 = 0.175
        assertEquals(0.175, res.expectedCalibrationError(), DELTA)
    }

    @Test
    fun `exact-match stream gives ECE 0`() {
        val r = ReliabilityStat(2).apply {
            update(0.25, 0.25)
            update(0.75, 0.75)
        }
        assertEquals(0.0, r.read(0L).expectedCalibrationError(), DELTA)
    }

    @Test
    fun `merge adds bin sums`() {
        val a = ReliabilityStat(3).apply {
            update(0.1, 1.0)
            update(0.5, 0.0)
        }
        val b = ReliabilityStat(3).apply {
            update(0.1, 0.0)
            update(0.9, 1.0)
        }
        a.merge(b.read(0L))
        val res = a.read(0L)
        assertEquals(2.0, res.totalWeights[0], DELTA)
        assertEquals(1.0, res.totalWeights[1], DELTA)
        assertEquals(1.0, res.totalWeights[2], DELTA)
    }
}
