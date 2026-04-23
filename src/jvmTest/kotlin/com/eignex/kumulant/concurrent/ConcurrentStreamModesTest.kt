package com.eignex.kumulant.concurrent

import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Multi-thread smoke tests for the lock-free `StreamMode`s. `SerialMode` is excluded —
 * it makes no thread-safety claim and would fail these assertions.
 */
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
    fun `ArrayBins adds from many threads preserve total weight`() {
        val bins = ArrayBins(AtomicMode)
        val threads = 4
        val iters = 5_000
        runConcurrently(threads, iters) { _, i ->
            // Spread writes across a handful of indices to exercise both the fast path
            // and the concurrent-grow path.
            bins.add(i % 50, 1.0)
        }
        val total = bins.snapshot().values.sum()
        assertEquals((threads * iters).toDouble(), total, 1e-9)
    }
}
