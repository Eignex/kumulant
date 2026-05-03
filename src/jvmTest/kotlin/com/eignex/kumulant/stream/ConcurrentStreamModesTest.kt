package com.eignex.kumulant.stream

import com.eignex.kumulant.locked.locked
import com.eignex.kumulant.stat.quantile.ReservoirHistogram
import com.eignex.kumulant.stat.quantile.TDigest
import com.eignex.kumulant.stat.sketch.SpaceSaving
import com.eignex.kumulant.stat.summary.Max
import com.eignex.kumulant.stat.summary.Mean
import com.eignex.kumulant.stat.summary.Min
import com.eignex.kumulant.stat.summary.Range
import com.eignex.kumulant.stat.summary.Variance
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.abs
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
    fun `Range under AtomicMode captures the true min and max under contention`() {
        val range = Range(AtomicMode)
        val threads = 8
        val iters = 5_000
        runConcurrently(threads, iters) { t, i ->
            range.update((t * iters + i).toDouble())
        }
        val result = range.read()
        assertEquals(0.0, result.min, 0.0)
        assertEquals((threads * iters - 1).toDouble(), result.max, 0.0)
    }

    @Test
    fun `Mean wrapped in locked preserves the Welford invariant under contention`() {
        val mean = Mean(AtomicMode).locked()
        val threads = 8
        val iters = 5_000
        runConcurrently(threads, iters) { t, i ->
            mean.update((t * iters + i).toDouble())
        }
        val result = mean.read()
        val n = (threads * iters).toLong()
        val expected = (n - 1).toDouble() / 2.0
        assertEquals(n.toDouble(), result.totalWeights, 0.0)
        kotlin.test.assertTrue(
            abs(result.mean - expected) < 1e-6,
            "mean drifted: got ${result.mean}, expected $expected",
        )
    }

    @Test
    fun `Variance wrapped in locked preserves the Welford invariant under contention`() {
        val variance = Variance(AtomicMode).locked()
        val threads = 8
        val iters = 5_000
        runConcurrently(threads, iters) { t, i ->
            variance.update((t * iters + i).toDouble())
        }
        val result = variance.read()
        val n = (threads * iters).toLong()
        val expectedMean = (n - 1).toDouble() / 2.0
        val expectedVar = (n.toDouble() * n - 1.0) / 12.0
        assertEquals(n.toDouble(), result.totalWeights, 0.0)
        kotlin.test.assertTrue(
            abs(result.mean - expectedMean) < 1e-6,
            "mean drifted: got ${result.mean}, expected $expectedMean",
        )
        kotlin.test.assertTrue(
            abs(result.variance - expectedVar) / expectedVar < 1e-9,
            "variance drifted: got ${result.variance}, expected $expectedVar",
        )
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

    @Test
    fun `TDigest survives concurrent update plus interleaved read with no exceptions`() {
        val tdigest = TDigest(compression = 100.0, mode = AtomicMode)
        val writers = 4
        val iters = 10_000
        runWithReader(tdigest::read) {
            runConcurrently(writers, iters) { t, i ->
                tdigest.update((t * iters + i).toDouble())
            }
        }
        val result = tdigest.read()
        // ~6 * compression is the documented centroid bound. Allow some slack.
        kotlin.test.assertTrue(
            result.means.size <= 8 * 100,
            "centroid count ${result.means.size} exceeded 8 * compression",
        )
        assertEquals((writers * iters).toDouble(), result.totalWeight, 1e-9)
    }

    @Test
    fun `ReservoirHistogram never exceeds capacity under concurrent update plus read`() {
        val capacity = 256
        val reservoir = ReservoirHistogram(capacity = capacity, seed = 42L, mode = AtomicMode)
        val writers = 4
        val iters = 10_000
        runWithReader(reservoir::read) {
            runConcurrently(writers, iters) { t, i ->
                reservoir.update((t * iters + i).toDouble())
            }
        }
        val result = reservoir.read()
        kotlin.test.assertTrue(
            result.values.size <= capacity,
            "reservoir size ${result.values.size} exceeded capacity $capacity",
        )
        assertEquals((writers * iters).toLong(), result.totalSeen)
    }

    @Test
    fun `SpaceSaving counts are non-decreasing under concurrent update plus read`() {
        val capacity = 64
        val ss = SpaceSaving(capacity = capacity, mode = AtomicMode)
        val writers = 4
        val iters = 10_000
        runWithReader(ss::read) {
            runConcurrently(writers, iters) { _, i ->
                // Skewed key distribution so eviction kicks in.
                ss.update((i % (capacity * 4)).toLong())
            }
        }
        val result = ss.read()
        kotlin.test.assertTrue(
            result.keys.size <= capacity,
            "tracked keys ${result.keys.size} exceeded capacity $capacity",
        )
        assertEquals((writers * iters).toLong(), result.totalSeen)
    }

    /** Run [block] with a parallel reader thread invoking [read] in a tight loop until [block] returns. */
    private fun <R> runWithReader(read: () -> R, block: () -> Unit) {
        val stop = AtomicBoolean(false)
        val pool = Executors.newSingleThreadExecutor()
        val started = CountDownLatch(1)
        val readerError = arrayOf<Throwable?>(null)
        try {
            pool.submit {
                started.countDown()
                try {
                    while (!stop.get()) read()
                } catch (t: Throwable) {
                    readerError[0] = t
                }
            }
            started.await()
            block()
        } finally {
            stop.set(true)
            pool.shutdown()
            check(pool.awaitTermination(10, TimeUnit.SECONDS)) { "reader did not stop in time" }
        }
        readerError[0]?.let { throw AssertionError("reader thread failed", it) }
    }
}
