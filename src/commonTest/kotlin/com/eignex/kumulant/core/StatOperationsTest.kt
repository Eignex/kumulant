package com.eignex.kumulant.core

import com.eignex.kumulant.stat.Mean
import com.eignex.kumulant.stat.Sum
import kotlin.test.Test
import kotlin.test.assertEquals

private const val DELTA = 1e-12

class StatOperationsTest {

    // --- mapSeries ---

    @Test
    fun `mapSeries applies transform before accumulation`() {
        val stat = Sum().mapSeries { it * 2.0 }
        stat.update(3.0)
        stat.update(5.0)
        assertEquals(16.0, stat.read().sum, DELTA)
    }

    @Test
    fun `mapSeries abs value`() {
        val stat = Sum().mapSeries { if (it < 0) -it else it }
        stat.update(-4.0)
        stat.update(3.0)
        assertEquals(7.0, stat.read().sum, DELTA)
    }

    // --- filter ---

    @Test
    fun `filter drops values that fail predicate`() {
        val stat = Sum().filter { it > 0.0 }
        stat.update(5.0)
        stat.update(-3.0)
        stat.update(2.0)
        assertEquals(7.0, stat.read().sum, DELTA)
    }

    @Test
    fun `filter passes all when predicate always true`() {
        val stat = Mean().filter { true }
        stat.update(1.0)
        stat.update(3.0)
        assertEquals(2.0, stat.read().mean, DELTA)
    }

    // --- onX / onY (PairedStat adapters) ---

    @Test
    fun `onX feeds x to underlying stat`() {
        val stat = Sum().onX()
        stat.update(10.0, 99.0) // x=10, y=99
        assertEquals(10.0, stat.read().sum, DELTA)
    }

    @Test
    fun `onY feeds y to underlying stat`() {
        val stat = Sum().onY()
        stat.update(99.0, 7.0) // x=99, y=7
        assertEquals(7.0, stat.read().sum, DELTA)
    }

    // --- mapFromPaired ---

    @Test
    fun `mapFromPaired computes difference y-x`() {
        val stat = Sum().mapFromPaired { x, y -> y - x }
        stat.update(3.0, 10.0)
        stat.update(1.0, 4.0)
        assertEquals(10.0, stat.read().sum, DELTA) // (10-3)+(4-1)=7+3=10
    }

    // --- atIndex ---

    @Test
    fun `atIndex picks single element from vector`() {
        val stat = Sum().atIndex(1)
        stat.update(doubleArrayOf(10.0, 20.0, 30.0))
        assertEquals(20.0, stat.read().sum, DELTA)
    }

    // --- atIndices ---

    @Test
    fun `atIndices extracts two elements for paired stat`() {
        val stat = Sum().onX().atIndices(0, 2)
        stat.update(doubleArrayOf(5.0, 99.0, 3.0))
        assertEquals(5.0, stat.read().sum, DELTA) // x=vec[0], y=vec[2]; onX takes x
    }

    // --- withFixedX / withFixedY ---

    @Test
    fun `withFixedX supplies constant x to paired stat`() {
        val stat = Sum().onX().withFixedX(7.0)
        stat.update(99.0) // y=99, but x is fixed to 7
        stat.update(99.0)
        assertEquals(14.0, stat.read().sum, DELTA)
    }

    @Test
    fun `withFixedY supplies constant y to paired stat`() {
        val stat = Sum().onY().withFixedY(3.0)
        stat.update(99.0) // x=99, but y is fixed to 3
        stat.update(99.0)
        assertEquals(6.0, stat.read().sum, DELTA)
    }

    // --- filter on PairedStat ---

    @Test
    fun `filter on paired stat drops pairs failing predicate`() {
        val stat = Sum().onX().filter { x, _ -> x > 0.0 }
        stat.update(5.0, 1.0)
        stat.update(-3.0, 1.0)
        assertEquals(5.0, stat.read().sum, DELTA)
    }

    // --- filter on VectorStat ---

    @Test
    fun `filter on vector stat drops vectors failing predicate`() {
        val stat = Sum().atIndex(0).filter { v -> v[0] > 0.0 }
        stat.update(doubleArrayOf(4.0))
        stat.update(doubleArrayOf(-2.0))
        assertEquals(4.0, stat.read().sum, DELTA)
    }

    // --- mapFromVector ---

    @Test
    fun `mapFromVector computes sum of elements`() {
        val stat = Sum().mapFromVector { v -> v.sum() }
        stat.update(doubleArrayOf(1.0, 2.0, 3.0))
        assertEquals(6.0, stat.read().sum, DELTA)
    }

    // --- copy preserves transform ---

    @Test
    fun `copy of mapSeries stat is independent`() {
        val s1 = Sum().mapSeries { it * 10.0 }
        s1.update(1.0)
        val s2 = s1.create()
        s2.update(1.0)
        assertEquals(10.0, s1.read().sum, DELTA)
        assertEquals(10.0, s2.read().sum, DELTA)
    }

    // --- reset delegates to underlying ---

    @Test
    fun `reset on mapSeries clears underlying stat`() {
        val stat = Sum().mapSeries { it * 2.0 }
        stat.update(5.0)
        stat.reset()
        assertEquals(0.0, stat.read().sum, DELTA)
    }
}
