package com.eignex.kumulant.stream

import com.eignex.kumulant.stat.summary.Max
import com.eignex.kumulant.stat.summary.Min
import kotlin.test.Test
import kotlin.test.assertEquals

class ConcurrentStreamModesTest {

    @Test
    fun `AtomicMode double sum is exact under contention`() {
        val d = AtomicMode.newDouble(0.0)
        val threads = 8
        val iters = 10_000
        runConcurrently(threads, iters) { _, _ ->
            d.add(1.0)
        }
        assertEquals((threads * iters).toDouble(), d.load(), 1e-9)
    }

    @Test
    fun `AtomicMode long sum is exact under contention`() {
        val l = AtomicMode.newLong(0L)
        val threads = 8
        val iters = 10_000
        runConcurrently(threads, iters) { _, _ ->
            l.add(1L)
        }
        assertEquals((threads * iters).toLong(), l.load())
    }

    @Test
    fun `FixedAtomicMode preserves exact integer-sized sums under contention`() {
        val mode = FixedAtomicMode(precision = 3)
        val d = mode.newDouble(0.0)
        val threads = 8
        val iters = 10_000
        runConcurrently(threads, iters) { _, _ ->
            d.add(1.0)
        }
        assertEquals((threads * iters).toDouble(), d.load(), 1e-6)
    }

    @Test
    fun `AtomicMode addAndGet always returns a monotonic sequence of snapshots`() {
        val l = AtomicMode.newLong(0L)
        val threads = 4
        val iters = 2_000
        runConcurrently(threads, iters) { _, _ ->
            val seen = l.addAndGet(1L)
            check(seen >= 1L) { "addAndGet returned $seen but all prior adds should have made it >= 1" }
        }
        assertEquals((threads * iters).toLong(), l.load())
    }

    @Test
    fun `AtomicDouble add to NaN cell publishes the result through CAS`() {
        val d = AtomicMode.newDouble(Double.NaN)
        d.add(1.0)
        kotlin.test.assertTrue(d.load().isNaN(), "NaN + 1.0 should still be NaN")
        val d2 = AtomicMode.newDouble(Double.POSITIVE_INFINITY)
        d2.add(1.0)
        kotlin.test.assertEquals(Double.POSITIVE_INFINITY, d2.load(), 0.0)
    }

    @Test
    fun `Min under AtomicMode captures the true minimum under contention`() {
        val min = Min(AtomicMode)
        val threads = 8
        val iters = 5_000
        runConcurrently(threads, iters) { t, i ->
            min.update((t * iters + i).toDouble())
        }
        assertEquals(0.0, min.read().min, 0.0)
    }

    @Test
    fun `Max under AtomicMode captures the true maximum under contention`() {
        val max = Max(AtomicMode)
        val threads = 8
        val iters = 5_000
        runConcurrently(threads, iters) { t, i ->
            max.update((t * iters + i).toDouble())
        }
        assertEquals((threads * iters - 1).toDouble(), max.read().max, 0.0)
    }

    @Test
    fun `ArrayBins adds from many threads preserve total weight`() {
        val bins = ArrayBins(AtomicMode)
        val threads = 4
        val iters = 5_000
        runConcurrently(threads, iters) { _, i ->

            bins.add(i % 50, 1.0)
        }
        val total = bins.snapshot().values.sum()
        assertEquals((threads * iters).toDouble(), total, 1e-9)
    }
}
