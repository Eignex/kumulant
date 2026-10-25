package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.DELTA
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

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
    fun `a rejected downdate leaves the extrema untouched`() {
        // The extrema are CAS cells with no inverse, so a throw that happens after they have moved
        // is unrecoverable.
        val s = SummaryStat()
        s.update(5.0, weight = 1.0)

        assertFailsWith<IllegalArgumentException> { s.update(1000.0, weight = -2.0) }

        val r = s.read()
        assertEquals(5.0, r.min, DELTA, "min moved on a rejected update")
        assertEquals(5.0, r.max, DELTA, "max moved on a rejected update")
        assertEquals(1.0, r.totalWeights, DELTA)
        assertEquals(5.0, r.mean, DELTA)
    }
}
