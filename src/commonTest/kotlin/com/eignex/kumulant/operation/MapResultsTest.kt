package com.eignex.kumulant.operation

import com.eignex.kumulant.core.MeanResult
import com.eignex.kumulant.core.SumResult
import com.eignex.kumulant.stat.Sum
import kotlin.test.Test
import kotlin.test.assertEquals

private const val DELTA = 1e-12

class MapResultsTest {

    private val forward: (SumResult) -> MeanResult = { MeanResult(it.sum) }
    private val reverse: (MeanResult) -> SumResult = { SumResult(it.mean) }

    @Test
    fun `series mapResult transforms read output`() {
        val stat = Sum().mapResult(forward, reverse)
        stat.update(3.0)
        stat.update(2.0)
        assertEquals(5.0, stat.read().mean, DELTA)
    }

    @Test
    fun `series mapResult merge uses reverse transform`() {
        val stat = Sum().mapResult(forward, reverse)
        stat.update(4.0)
        stat.merge(MeanResult(6.0))
        assertEquals(10.0, stat.read().mean, DELTA)
    }

    @Test
    fun `series mapResult create is independent`() {
        val s1 = Sum().mapResult(forward, reverse)
        s1.update(2.0)
        val s2 = s1.create()
        s2.update(5.0)

        assertEquals(2.0, s1.read().mean, DELTA)
        assertEquals(5.0, s2.read().mean, DELTA)
    }

    @Test
    fun `series mapResult reset clears state`() {
        val stat = Sum().mapResult(forward, reverse)
        stat.update(8.0)
        stat.reset()
        assertEquals(0.0, stat.read().mean, DELTA)
    }

    @Test
    fun `paired mapResult delegates updates and transforms read`() {
        val stat = Sum().atX().mapResult(forward, reverse)
        stat.update(7.0, 100.0)
        stat.update(3.0, 200.0)
        assertEquals(10.0, stat.read().mean, DELTA)
    }

    @Test
    fun `paired mapResult merge uses reverse transform`() {
        val stat = Sum().atX().mapResult(forward, reverse)
        stat.update(1.0, 1.0)
        stat.merge(MeanResult(4.0))
        assertEquals(5.0, stat.read().mean, DELTA)
    }

    @Test
    fun `vector mapResult delegates updates and transforms read`() {
        val stat = Sum().atIndex(1).mapResult(forward, reverse)
        stat.update(doubleArrayOf(1.0, 3.0, 9.0))
        stat.update(doubleArrayOf(2.0, 5.0, 8.0))
        assertEquals(8.0, stat.read().mean, DELTA)
    }

    @Test
    fun `vector mapResult merge uses reverse transform`() {
        val stat = Sum().atIndex(0).mapResult(forward, reverse)
        stat.update(doubleArrayOf(2.0))
        stat.merge(MeanResult(3.0))
        assertEquals(5.0, stat.read().mean, DELTA)
    }

    @Test
    fun `discrete mapResult round-trips a SumResult through cast bridge`() {
        // Bridge a Discrete Sum (via asDiscrete) and remap its result to MeanResult
        // and back, verifying both forward (read) and reverse (merge) paths.
        val stat = com.eignex.kumulant.stat.Sum().asDiscrete().mapResult(forward, reverse)
        stat.update(2L)
        stat.update(3L)
        assertEquals(5.0, stat.read().mean, DELTA)

        stat.merge(MeanResult(10.0))
        assertEquals(15.0, stat.read().mean, DELTA)
    }
}
