package com.eignex.kumulant.stat.summary

import kotlin.test.Test
import kotlin.test.assertEquals

private const val DELTA = 1e-12

class MeanStatTest {
    @Test
    fun `create produces fresh independent stat`() {
        val m1 = MeanStat().apply { update(10.0) }
        val m2 = m1.create()
        m1.update(20.0)
        assertEquals(15.0, m1.read().mean, DELTA)
        assertEquals(0.0, m2.read().totalWeights, DELTA)
    }

    @Test
    fun `test stability with large offset`() {
        val mean = MeanStat()
        val offset = 1e9

        mean.update(offset + 1, 1.0)
        mean.update(offset + 2, 1.0)
        mean.update(offset + 3, 1.0)
        assertEquals(offset + 2.0, mean.read().mean, DELTA)
    }

    @Test
    fun `test zero weight updates`() {
        val mean = MeanStat()
        mean.update(10.0, 1.0)
        mean.update(100.0, 0.0)
        assertEquals(10.0, mean.read().mean, DELTA)
        assertEquals(1.0, mean.read().totalWeights, DELTA)
    }

    @Test
    fun `test weighted balance`() {
        val mean = MeanStat()
        mean.update(10.0, 90.0)
        mean.update(100.0, 10.0)
        assertEquals(19.0, mean.read().mean, DELTA)
    }

    @Test
    fun `test empty merge`() {
        val mean = MeanStat()
        mean.update(5.0, 1.0)
        mean.merge(WeightedMeanResult(0.0, 100.0))
        assertEquals(5.0, mean.read().mean, DELTA)
    }

    @Test
    fun `test negative values`() {
        val mean = MeanStat()
        mean.update(-10.0)
        mean.update(-20.0)
        assertEquals(-15.0, mean.read().mean, DELTA)
    }

    @Test
    fun `test reset`() {
        val mean = MeanStat()
        mean.update(50.0)
        mean.reset()
        assertEquals(0.0, mean.read().mean, DELTA)
        assertEquals(0.0, mean.read().totalWeights, DELTA)
    }
}
