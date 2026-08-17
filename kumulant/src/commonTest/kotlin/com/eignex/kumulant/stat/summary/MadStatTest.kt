package com.eignex.kumulant.stat.summary

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class MadStatTest {

    @Test
    fun `read before any update reports NaN estimates`() {
        // MadStat is built on two TDigestStats, so it inherits their empty-read sentinel.
        val r = MadStat().read()
        assertTrue(r.median.isNaN(), "median was ${r.median}")
        assertTrue(r.mad.isNaN(), "mad was ${r.mad}")
    }

    @Test
    fun `compression must be positive`() {
        assertFailsWith<IllegalArgumentException> { MadStat(compression = 0.0) }
        assertFailsWith<IllegalArgumentException> { MadStat(compression = -1.0) }
    }

    @Test
    fun `median tracks the central value of a symmetric stream`() {
        val s = MadStat(compression = 200.0)
        for (i in 0 until 1000) s.update((i % 100).toDouble())
        val r = s.read()
        assertEquals(50.0, r.median, 2.0)
    }

    @Test
    fun `mad approaches the theoretical value on a centred stream`() {
        // Values uniform in [-1, 1] -> median 0, |x| uniform in [0, 1] -> median |x| ~ 0.5.
        val s = MadStat(compression = 200.0)
        val n = 5_000
        for (i in 0 until n) {
            val u = -1.0 + 2.0 * (i.toDouble() / (n - 1).toDouble())
            s.update(u)
        }
        val r = s.read()
        assertEquals(0.0, r.median, 0.05)
        assertEquals(0.5, r.mad, 0.05)
    }

    @Test
    fun `reset clears state`() {
        val s = MadStat().apply { for (i in 0 until 100) update(i.toDouble()) }
        s.reset()
        val r = s.read()
        // A reset stat is empty again, and an empty sample has no median.
        assertTrue(r.median.isNaN(), "median was ${r.median}")
        assertTrue(r.mad.isNaN(), "mad was ${r.mad}")
    }

    @Test
    fun `create produces an independent stat`() {
        val tpl = MadStat().apply { for (i in 0 until 100) update(i.toDouble()) }
        val fresh = tpl.create()
        assertTrue(fresh.read().median.isNaN(), "a fresh stat has seen nothing, so it has no median")
        assertTrue(tpl.read().median.isFinite(), "the populated template still reports a median")
    }
}
