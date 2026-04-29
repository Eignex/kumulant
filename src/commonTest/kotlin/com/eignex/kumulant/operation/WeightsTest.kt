package com.eignex.kumulant.operation

import com.eignex.kumulant.stat.cardinality.HyperLogLog

import com.eignex.kumulant.stat.summary.Sum

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private const val DELTA = 1e-12
private fun sumVector(d: Int) = VectorizedStat(d, template = { Sum() })

class WeightsTest {

    @Test
    fun `series withWeight overrides caller weight`() {
        val stat = Sum().withWeight(2.0)
        stat.update(3.0, weight = 100.0) // caller weight ignored
        assertEquals(6.0, stat.read().sum, DELTA)
    }

    @Test
    fun `series withWeight create preserves weight`() {
        val template = Sum().withWeight(3.0)
        val fresh = template.create()
        fresh.update(2.0)
        assertEquals(6.0, fresh.read().sum, DELTA)
    }

    @Test
    fun `paired withWeight overrides caller weight`() {
        val stat = Sum().atY().withWeight(2.0)
        stat.update(0.0, 5.0)
        assertEquals(10.0, stat.read().sum, DELTA)
    }

    @Test
    fun `vector withWeight overrides caller weight`() {
        val stat = sumVector(2).withWeight(2.0)
        stat.update(doubleArrayOf(3.0, 4.0))
        assertEquals(6.0, stat.read().results[0].sum, DELTA)
        assertEquals(8.0, stat.read().results[1].sum, DELTA)
    }

    @Test
    fun `discrete withWeight at zero drops all updates`() {
        val stat = HyperLogLog(precision = 10).withWeight(0.0)
        for (i in 1L..100L) stat.update(i, weight = 1.0)
        assertEquals(0.0, stat.read().estimate)
    }

    @Test
    fun `discrete withWeight overrides caller weight`() {
        val stat = HyperLogLog(precision = 10).withWeight(1.0)
        for (i in 1L..50L) stat.update(i, weight = 0.0) // caller weight ignored
        assertTrue(stat.read().estimate > 30.0)
    }
}
