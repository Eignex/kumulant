package com.eignex.kumulant.concurrent

import com.eignex.kumulant.operation.VectorizedStat
import com.eignex.kumulant.stat.OLS
import com.eignex.kumulant.stat.Sum
import kotlin.test.Test
import kotlin.test.assertEquals

private const val DELTA = 1e-9

class LockedSeriesStatTest {

    @Test
    fun `delegates update, read, and reset to the wrapped stat`() {
        val locked = Sum(mode = AtomicMode).locked()
        locked.update(1.0)
        locked.update(2.5)
        assertEquals(3.5, locked.read().sum, DELTA)

        locked.reset()
        assertEquals(0.0, locked.read().sum, DELTA)
    }

    @Test
    fun `merge is delegated under the write lock`() {
        val a = Sum(mode = AtomicMode).locked()
        a.update(1.0)

        val b = Sum(mode = AtomicMode)
        b.update(2.0)
        a.merge(b.read())

        assertEquals(3.0, a.read().sum, DELTA)
    }

    @Test
    fun `concurrent updates preserve total count without lost writes`() {
        val locked = Sum(mode = AtomicMode).locked()
        val threads = 8
        val iters = 10_000

        runConcurrently(threads, iters) { _, _ ->
            locked.update(1.0)
        }

        assertEquals((threads * iters).toDouble(), locked.read().sum, DELTA)
    }

    @Test
    fun `create returns a LockedSeriesStat wrapping a fresh delegate`() {
        val original = Sum(mode = AtomicMode).locked()
        original.update(5.0)

        val clone = original.create()
        clone.update(3.0)

        assertEquals(5.0, original.read().sum, DELTA)
        assertEquals(3.0, clone.read().sum, DELTA)
    }
}

class LockedPairedStatTest {

    @Test
    fun `delegates paired updates to the wrapped stat`() {
        val locked = OLS(mode = AtomicMode).locked()
        locked.update(1.0, 2.0)
        locked.update(2.0, 4.0)
        locked.update(3.0, 6.0)
        val r = locked.read()
        assertEquals(2.0, r.slope, DELTA)
    }

    @Test
    fun `concurrent paired updates preserve total weights`() {
        val locked = OLS(mode = AtomicMode).locked()
        val threads = 4
        val iters = 5_000
        runConcurrently(threads, iters) { t, i ->
            locked.update(x = (t + i).toDouble(), y = (t + i).toDouble())
        }
        assertEquals((threads * iters).toDouble(), locked.read().totalWeights, DELTA)
    }

    @Test
    fun `reset clears and create returns fresh`() {
        val original = OLS(mode = AtomicMode).locked()
        original.update(1.0, 2.0)
        original.reset()
        assertEquals(0.0, original.read().totalWeights, DELTA)

        val clone = original.create()
        clone.update(1.0, 2.0)
        assertEquals(0.0, original.read().totalWeights, DELTA)
        assertEquals(1.0, clone.read().totalWeights, DELTA)
    }
}

class LockedVectorStatTest {

    @Test
    fun `delegates vector updates`() {
        val locked = VectorizedStat(2, template = { Sum(mode = AtomicMode) }).locked()
        locked.update(doubleArrayOf(1.0, 2.0))
        locked.update(doubleArrayOf(3.0, 4.0))
        val r = locked.read()
        assertEquals(4.0, r.results[0].sum, DELTA)
        assertEquals(6.0, r.results[1].sum, DELTA)
    }

    @Test
    fun `concurrent vector updates preserve per-dimension totals`() {
        val locked = VectorizedStat(3, template = { Sum(mode = AtomicMode) }).locked()
        val threads = 4
        val iters = 5_000
        runConcurrently(threads, iters) { _, _ ->
            locked.update(doubleArrayOf(1.0, 2.0, 3.0))
        }
        val r = locked.read()
        val expectedBase = (threads * iters).toDouble()
        assertEquals(expectedBase, r.results[0].sum, DELTA)
        assertEquals(expectedBase * 2, r.results[1].sum, DELTA)
        assertEquals(expectedBase * 3, r.results[2].sum, DELTA)
    }

    @Test
    fun `reset and create work on the locked vector`() {
        val original = VectorizedStat(2, template = { Sum(mode = AtomicMode) }).locked()
        original.update(doubleArrayOf(1.0, 2.0))
        original.reset()
        assertEquals(0.0, original.read().results[0].sum, DELTA)

        val clone = original.create()
        clone.update(doubleArrayOf(5.0, 6.0))
        assertEquals(0.0, original.read().results[0].sum, DELTA)
        assertEquals(5.0, clone.read().results[0].sum, DELTA)
    }
}
