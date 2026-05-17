package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stat.summary.SumStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

private const val DELTA = 1e-12

private fun sumVector(dimensions: Int): VectorizedStat<SumResult> =
    VectorizedStat(dimensions, SumStat())

class VectorizedStatTest {

    @Test
    fun `updates each dimension independently`() {
        val stat = sumVector(3)
        stat.update(doubleArrayOf(1.0, 2.0, 3.0))
        stat.update(doubleArrayOf(4.0, 5.0, 6.0))
        val r = stat.read()
        assertEquals(5.0, r.results[0].sum, DELTA)
        assertEquals(7.0, r.results[1].sum, DELTA)
        assertEquals(9.0, r.results[2].sum, DELTA)
    }

    @Test
    fun `wrong vector size throws`() {
        val stat = sumVector(3)
        assertFailsWith<IllegalArgumentException> {
            stat.update(doubleArrayOf(1.0, 2.0))
        }
    }

    @Test
    fun `merge combines each dimension`() {
        val s1 = sumVector(2).apply { update(doubleArrayOf(1.0, 2.0)) }
        val s2 = sumVector(2).apply { update(doubleArrayOf(3.0, 4.0)) }
        s1.merge(s2.read())
        val r = s1.read()
        assertEquals(4.0, r.results[0].sum, DELTA)
        assertEquals(6.0, r.results[1].sum, DELTA)
    }

    @Test
    fun `reset clears all dimensions`() {
        val stat = sumVector(2)
        stat.update(doubleArrayOf(5.0, 10.0))
        stat.reset()
        val r = stat.read()
        assertEquals(0.0, r.results[0].sum, DELTA)
        assertEquals(0.0, r.results[1].sum, DELTA)
    }

    @Test
    fun `concurrency reflects template when no override is given`() {
        // Without an explicit override the wrapper must report the template's mode,
        // not Concurrency.None - children are built from the template and inherit
        // its mode, so the trait would otherwise lie.
        val stat = VectorizedStat(2, SumStat(concurrency = Concurrency.Relaxed))
        assertEquals(Concurrency.Relaxed, stat.concurrency)
    }

    @Test
    fun `create propagates a new concurrency mode through the template`() {
        val original = VectorizedStat(2, SumStat(concurrency = Concurrency.Relaxed))
        val derived = original.create(Concurrency.Strict)
        assertEquals(Concurrency.Strict, derived.concurrency)
    }

    @Test
    fun `weighted update applies weight to each dimension`() {
        val stat = sumVector(2)
        stat.update(doubleArrayOf(1.0, 1.0), weight = 3.0)
        val r = stat.read()
        assertEquals(3.0, r.results[0].sum, DELTA)
        assertEquals(3.0, r.results[1].sum, DELTA)
    }
}
