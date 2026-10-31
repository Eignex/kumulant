package com.eignex.kumulant.operation

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.stat.summary.MaxResult
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stat.summary.SumStat
import kotlin.test.Test
import kotlin.test.assertEquals

class MapResultsTest {

    private val forward: (SumResult) -> MaxResult = { MaxResult(it.sum) }
    private val reverse: (MaxResult) -> SumResult = { SumResult(it.max) }

    @Test
    fun `series mapResult transforms read output`() {
        val stat = SumStat().mapResult(forward, reverse)
        stat.update(3.0)
        stat.update(2.0)
        assertEquals(5.0, stat.read().max, DELTA)
    }

    @Test
    fun `series mapResult merge uses reverse transform`() {
        val stat = SumStat().mapResult(forward, reverse)
        stat.update(4.0)
        stat.merge(MaxResult(6.0))
        assertEquals(10.0, stat.read().max, DELTA)
    }

    @Test
    fun `series mapResult create is independent`() {
        val s1 = SumStat().mapResult(forward, reverse)
        s1.update(2.0)
        val s2 = s1.create()
        s2.update(5.0)

        assertEquals(2.0, s1.read().max, DELTA)
        assertEquals(5.0, s2.read().max, DELTA)
    }

    @Test
    fun `series mapResult reset clears state`() {
        val stat = SumStat().mapResult(forward, reverse)
        stat.update(8.0)
        stat.reset()
        assertEquals(0.0, stat.read().max, DELTA)
    }

    @Test
    fun `paired mapResult delegates updates and transforms read`() {
        val stat = SumStat().atX().mapResult(forward, reverse)
        stat.update(7.0, 100.0)
        stat.update(3.0, 200.0)
        assertEquals(10.0, stat.read().max, DELTA)
    }

    @Test
    fun `paired mapResult merge uses reverse transform`() {
        val stat = SumStat().atX().mapResult(forward, reverse)
        stat.update(1.0, 1.0)
        stat.merge(MaxResult(4.0))
        assertEquals(5.0, stat.read().max, DELTA)
    }

    @Test
    fun `vector mapResult delegates updates and transforms read`() {
        val stat = SumStat().atIndex(1).mapResult(forward, reverse)
        stat.update(doubleArrayOf(1.0, 3.0, 9.0))
        stat.update(doubleArrayOf(2.0, 5.0, 8.0))
        assertEquals(8.0, stat.read().max, DELTA)
    }

    @Test
    fun `vector mapResult merge uses reverse transform`() {
        val stat = SumStat().atIndex(0).mapResult(forward, reverse)
        stat.update(doubleArrayOf(2.0))
        stat.merge(MaxResult(3.0))
        assertEquals(5.0, stat.read().max, DELTA)
    }

    @Test
    fun `discrete mapResult round-trips a SumResult through cast bridge`() {
        val stat = com.eignex.kumulant.stat.summary.SumStat().asDiscrete().mapResult(forward, reverse)
        stat.update(2L)
        stat.update(3L)
        assertEquals(5.0, stat.read().max, DELTA)

        stat.merge(MaxResult(10.0))
        assertEquals(15.0, stat.read().max, DELTA)
    }
}
