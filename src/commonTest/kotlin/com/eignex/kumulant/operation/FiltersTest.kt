package com.eignex.kumulant.operation

import com.eignex.kumulant.stat.cardinality.HyperLogLog
import com.eignex.kumulant.stat.cardinality.LinearCounting
import com.eignex.kumulant.stat.summary.Sum
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private const val DELTA = 1e-12
private fun sumVector(d: Int) = VectorizedStat(d, template = { Sum() })

class FiltersTest {

    @Test
    fun `series filter gates updates by predicate`() {
        val stat = Sum().filter { it > 0.0 }
        stat.update(-1.0)
        stat.update(2.0)
        stat.update(3.0)
        assertEquals(5.0, stat.read().sum, DELTA)
    }

    @Test
    fun `series filter create preserves predicate`() {
        val template = Sum().filter { it > 0.0 }
        val fresh = template.create()
        fresh.update(-1.0)
        fresh.update(4.0)
        assertEquals(4.0, fresh.read().sum, DELTA)
    }

    @Test
    fun `paired filter gates updates`() {
        val stat = Sum().atX().filter { x, y -> x + y > 0.0 }
        stat.update(-3.0, 1.0) // dropped
        stat.update(2.0, 5.0)
        stat.update(7.0, -1.0)
        assertEquals(9.0, stat.read().sum, DELTA)
    }

    @Test
    fun `vector filter gates updates`() {
        val stat = sumVector(2).filter { it[0] > 0.0 }
        stat.update(doubleArrayOf(-1.0, 100.0)) // dropped
        stat.update(doubleArrayOf(2.0, 3.0))
        stat.update(doubleArrayOf(4.0, 5.0))
        val r = stat.read()
        assertEquals(6.0, r.results[0].sum, DELTA)
        assertEquals(8.0, r.results[1].sum, DELTA)
    }

    @Test
    fun `discrete filter gates updates`() {
        val stat = HyperLogLog(precision = 10).filter { it >= 0L }
        for (i in -50L..50L) stat.update(i)
        val seen = stat.read().estimate
        assertTrue(seen in 45.0..56.0, "estimate=$seen")
    }

    @Test
    fun `discrete filter create preserves predicate`() {
        val template = LinearCounting(bits = 1024).filter { it % 2L == 0L }
        val fresh = template.create()
        for (i in 1L..100L) fresh.update(i)
        val seen = fresh.read().estimate
        assertTrue(seen in 40.0..60.0, "estimate=$seen")
    }

    @Test
    fun `LongPredicate fun-interface lambda compiles and runs`() {
        val pred = LongPredicate { it > 5L }
        assertEquals(true, pred.test(10L))
        assertEquals(false, pred.test(3L))
    }
}

class FilterEdgeCasesTest {

    @Test
    fun `series filter rejecting all leaves stat in zero state`() {
        val stat = Sum().filter { false }
        stat.update(1.0)
        stat.update(2.0)
        stat.update(3.0)
        assertEquals(0.0, stat.read().sum, DELTA)
    }

    @Test
    fun `paired filter rejecting all leaves stat in zero state`() {
        val stat = Sum().atX().filter { _, _ -> false }
        stat.update(1.0, 2.0)
        stat.update(3.0, 4.0)
        assertEquals(0.0, stat.read().sum, DELTA)
    }

    @Test
    fun `vector filter rejecting all leaves stat in zero state`() {
        val stat = Sum().atIndex(0).filter { _ -> false }
        stat.update(doubleArrayOf(1.0))
        stat.update(doubleArrayOf(2.0))
        assertEquals(0.0, stat.read().sum, DELTA)
    }

    @Test
    fun `series filter excludes NaN values`() {
        val stat = Sum().filter { !it.isNaN() }
        stat.update(1.0)
        stat.update(Double.NaN)
        stat.update(2.0)
        assertEquals(3.0, stat.read().sum, DELTA)
    }

    @Test
    fun `paired filter excludes pairs containing NaN`() {
        val stat = Sum().atY().filter { x, y -> !x.isNaN() && !y.isNaN() }
        stat.update(1.0, 10.0)
        stat.update(Double.NaN, 20.0)
        stat.update(3.0, Double.NaN)
        stat.update(4.0, 30.0)
        assertEquals(40.0, stat.read().sum, DELTA)
    }

    @Test
    fun `vector filter excludes vectors containing NaN`() {
        val stat = Sum().atIndex(0).filter { v -> v.none { it.isNaN() } }
        stat.update(doubleArrayOf(1.0, 2.0))
        stat.update(doubleArrayOf(Double.NaN, 3.0))
        stat.update(doubleArrayOf(4.0, 5.0))
        assertEquals(5.0, stat.read().sum, DELTA)
    }

    @Test
    fun `series filter passes all when predicate always true`() {
        val stat = com.eignex.kumulant.stat.summary.Mean().filter { true }
        stat.update(1.0)
        stat.update(3.0)
        assertEquals(2.0, stat.read().mean, DELTA)
    }
}
