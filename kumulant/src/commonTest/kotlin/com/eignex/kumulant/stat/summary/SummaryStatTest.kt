package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.HasCenterScale
import com.eignex.kumulant.core.HasMinMax
import com.eignex.kumulant.core.HasSampleVariance
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private const val DELTA = 1e-9

class SummaryStatTest {

    @Test
    fun `tracks mean variance and range together`() {
        val s = SummaryStat()
        for (v in listOf(1.0, 2.0, 3.0, 4.0, 5.0)) s.update(v)
        val r = s.read()
        assertEquals(3.0, r.mean, DELTA)
        assertEquals(2.0, r.variance, DELTA) // population variance of 1..5
        assertEquals(1.0, r.min, DELTA)
        assertEquals(5.0, r.max, DELTA)
    }

    @Test
    fun `result implements both HasCenterScale and HasMinMax`() {
        val s = SummaryStat()
        for (v in 1..4) s.update(v.toDouble())
        val r = s.read()
        assertTrue(r is HasCenterScale)
        assertTrue(r is HasMinMax)
        assertTrue(r is HasSampleVariance)
        assertEquals(r.mean, r.center, DELTA)
        assertEquals(r.stdDev, r.scale, DELTA)
    }

    @Test
    fun `empty stream produces zeroes not infinities`() {
        val r = SummaryStat().read()
        assertEquals(0.0, r.mean, DELTA)
        assertEquals(0.0, r.variance, DELTA)
        assertEquals(0.0, r.min, DELTA)
        assertEquals(0.0, r.max, DELTA)
    }

    @Test
    fun `reset clears all cells`() {
        val s = SummaryStat().apply { for (v in 1..5) update(v.toDouble()) }
        s.reset()
        val r = s.read()
        assertEquals(0.0, r.totalWeights, DELTA)
        assertEquals(0.0, r.mean, DELTA)
        assertEquals(0.0, r.min, DELTA)
        assertEquals(0.0, r.max, DELTA)
    }

    @Test
    fun `create produces an independent stat`() {
        val tpl = SummaryStat().apply {
            update(7.0)
            update(3.0)
        }
        val fresh = tpl.create()
        assertEquals(0.0, fresh.read().mean, DELTA)
        assertEquals(5.0, tpl.read().mean, DELTA)
    }
}
