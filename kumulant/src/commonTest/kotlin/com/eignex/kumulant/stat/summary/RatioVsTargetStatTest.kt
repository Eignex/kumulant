package com.eignex.kumulant.stat.summary

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private const val DELTA = 1e-12

class RatioVsTargetStatTest {

    @Test
    fun `empty stream reports NaN ratio`() {
        val r = RatioVsTargetStat(threshold = 0.5).read()
        assertEquals(0.0, r.matched)
        assertEquals(0.0, r.total)
        assertTrue(r.ratio.isNaN())
    }

    @Test
    fun `AtLeast counts values greater than or equal to the threshold`() {
        val s = RatioVsTargetStat(threshold = 0.5, comparison = TargetComparison.AtLeast)
        listOf(0.1, 0.5, 0.7, 0.3, 0.9).forEach { s.update(it) }
        val r = s.read()
        assertEquals(3.0, r.matched, DELTA)
        assertEquals(5.0, r.total, DELTA)
        assertEquals(0.6, r.ratio, DELTA)
    }

    @Test
    fun `Above is strictly greater than the threshold`() {
        val s = RatioVsTargetStat(threshold = 0.5, comparison = TargetComparison.Above)
        listOf(0.5, 0.7, 0.5).forEach { s.update(it) }
        assertEquals(1.0, s.read().matched, DELTA)
    }

    @Test
    fun `Below and AtMost differ at the threshold`() {
        val below = RatioVsTargetStat(threshold = 0.5, comparison = TargetComparison.Below)
        val atMost = RatioVsTargetStat(threshold = 0.5, comparison = TargetComparison.AtMost)
        listOf(0.3, 0.5, 0.7).forEach {
            below.update(it)
            atMost.update(it)
        }
        assertEquals(1.0, below.read().matched, DELTA)
        assertEquals(2.0, atMost.read().matched, DELTA)
    }

    @Test
    fun `Equals matches only exact threshold`() {
        val s = RatioVsTargetStat(threshold = 0.5, comparison = TargetComparison.Equals)
        listOf(0.5, 0.5, 0.7).forEach { s.update(it) }
        assertEquals(2.0, s.read().matched, DELTA)
    }

    @Test
    fun `weighted updates accumulate weight not count`() {
        val s = RatioVsTargetStat(threshold = 0.0, comparison = TargetComparison.AtLeast)
        s.update(value = 1.0, weight = 2.0)
        s.update(value = -1.0, weight = 3.0)
        val r = s.read()
        assertEquals(2.0, r.matched, DELTA)
        assertEquals(5.0, r.total, DELTA)
        assertEquals(2.0 / 5.0, r.ratio, DELTA)
    }

    @Test
    fun `merge sums matched and total`() {
        val a = RatioVsTargetStat(threshold = 0.5).apply {
            listOf(0.6, 0.4).forEach { update(it) }
        }
        val b = RatioVsTargetStat(threshold = 0.5).apply {
            listOf(0.9, 0.7, 0.1).forEach { update(it) }
        }
        a.merge(b.read())
        val r = a.read()
        assertEquals(3.0, r.matched, DELTA)
        assertEquals(5.0, r.total, DELTA)
    }

    @Test
    fun `reset clears state`() {
        val s = RatioVsTargetStat(threshold = 0.5).apply { update(0.7) }
        s.reset()
        val r = s.read()
        assertEquals(0.0, r.matched, DELTA)
        assertEquals(0.0, r.total, DELTA)
    }

    @Test
    fun `create produces an independent stat`() {
        val tpl = RatioVsTargetStat(threshold = 0.5).apply { update(0.9) }
        val fresh = tpl.create()
        fresh.update(0.1)
        assertEquals(1.0, tpl.read().matched, DELTA)
        assertEquals(0.0, fresh.read().matched, DELTA)
    }
}
