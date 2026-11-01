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
    fun `mapResult create is independent`() {
        val s1 = SumStat().atX().mapResult(forward, reverse)
        s1.update(2.0, 0.0)
        val s2 = s1.create()
        s2.update(5.0, 0.0)

        assertEquals(2.0, s1.read().max, DELTA)
        assertEquals(5.0, s2.read().max, DELTA)
    }

    @Test
    fun `mapResult reset clears state`() {
        val stat = SumStat().atX().mapResult(forward, reverse)
        stat.update(8.0, 0.0)
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
}
