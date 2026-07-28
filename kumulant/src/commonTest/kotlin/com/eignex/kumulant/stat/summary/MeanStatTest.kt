package com.eignex.kumulant.stat.summary

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

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
    fun `negative weight downdates back to the prior state`() {
        val mean = MeanStat()
        mean.update(10.0, 2.0)
        mean.update(30.0, 1.5)
        val before = mean.read()

        mean.update(7.0, 3.0)
        mean.update(7.0, -3.0)

        val after = mean.read()
        assertEquals(before.totalWeights, after.totalWeights, DELTA)
        assertEquals(before.mean, after.mean, DELTA)
    }

    @Test
    fun `downdate drives a sliding window`() {
        val mean = MeanStat()
        listOf(1.0, 2.0, 3.0, 4.0).forEach { mean.update(it, 1.0) }
        // Drop the two oldest, leaving mean(3, 4).
        mean.update(1.0, -1.0)
        mean.update(2.0, -1.0)
        val r = mean.read()
        assertEquals(2.0, r.totalWeights, DELTA)
        assertEquals(3.5, r.mean, DELTA)
    }

    @Test
    fun `a downdate that would exhaust the accumulated weight throws`() {
        val mean = MeanStat()
        mean.update(1.0, 1.0)
        assertFailsWith<IllegalArgumentException> { mean.update(2.0, -1.0) }
        assertFailsWith<IllegalArgumentException> { mean.update(2.0, -5.0) }
        val r = mean.read()
        assertEquals(1.0, r.mean, DELTA)
        assertEquals(1.0, r.totalWeights, DELTA)
        assertTrue(r.mean.isFinite())
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
