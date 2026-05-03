package com.eignex.kumulant.stream

import com.eignex.kumulant.stat.cardinality.HyperLogLog
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

private const val DELTA = 1e-9

class AdderModeTest {

    @Test
    fun `DoubleAdder round-trips load and store`() {
        val d = AdderMode.newDouble(0.0)
        d.store(2.5)
        assertEquals(2.5, d.load(), DELTA)
    }

    @Test
    fun `DoubleAdder add accumulates`() {
        val d = AdderMode.newDouble(0.0)
        d.add(1.5)
        d.add(2.5)
        assertEquals(4.0, d.load(), DELTA)
    }

    @Test
    fun `DoubleAdder addAndGet returns updated value`() {
        val d = AdderMode.newDouble(1.0)
        assertEquals(3.5, d.addAndGet(2.5), DELTA)
        assertEquals(3.5, d.load(), DELTA)
    }

    @Test
    fun `DoubleAdder getAndAdd returns previous value`() {
        val d = AdderMode.newDouble(1.0)
        assertEquals(1.0, d.getAndAdd(2.5), DELTA)
        assertEquals(3.5, d.load(), DELTA)
    }

    @Test
    fun `DoubleAdder store resets then assigns`() {
        val d = AdderMode.newDouble(100.0)
        d.store(7.0)
        assertEquals(7.0, d.load(), DELTA)
    }

    @Test
    fun `LongAdder round-trips and accumulates`() {
        val l = AdderMode.newLong(0L)
        l.add(3L)
        l.add(7L)
        assertEquals(10L, l.load())
        assertEquals(15L, l.addAndGet(5L))
        assertEquals(15L, l.getAndAdd(0L))
    }

    @Test
    fun `concurrent DoubleAdder writes sum exactly`() {
        val d = AdderMode.newDouble(0.0)
        val threads = 8
        val iters = 10_000
        runConcurrently(threads, iters) { _, _ ->
            d.add(1.0)
        }
        assertEquals((threads * iters).toDouble(), d.load(), DELTA)
    }

    @Test
    fun `concurrent LongAdder writes sum exactly`() {
        val l = AdderMode.newLong(0L)
        val threads = 8
        val iters = 10_000
        runConcurrently(threads, iters) { _, _ ->
            l.add(1L)
        }
        assertEquals((threads * iters).toLong(), l.load())
    }

    @Test
    fun `AdderMode reference delegates to AtomicReference`() {
        val ref = AdderMode.newReference("a")
        check(ref.compareAndSet("a", "b"))
        assertEquals("b", ref.load())
    }

    @Test
    fun `LongAdder compareAndSet throws UnsupportedOperationException`() {
        val l = AdderMode.newLong(0L)
        assertFailsWith<UnsupportedOperationException> {
            l.compareAndSet(0L, 1L)
        }
    }

    @Test
    fun `HyperLogLog runs under AdderMode via the array-cell fallback`() {
        // HLL's registers are a StreamLongArray; AdderMode delegates array allocation
        // to AtomicMode-style atomics, so casMax works and update() succeeds.
        val hll = HyperLogLog(precision = 10, mode = AdderMode)
        for (i in 1..1000L) hll.update(i)
        val result = hll.read()
        assert(result.estimate > 0.0) { "expected non-zero estimate, got ${result.estimate}" }
    }
}
