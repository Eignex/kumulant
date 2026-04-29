package com.eignex.kumulant.operation

import com.eignex.kumulant.stat.Mean
import com.eignex.kumulant.stat.Sum
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds

private const val DELTA = 1e-12

private const val T0 = 0L
private const val T3 = 3_000_000_000L
private const val T9 = 9_000_000_000L
private const val T10 = 10_000_000_000L
private const val T11 = 11_000_000_000L
private const val T20 = 20_000_000_000L

class WindowedStatsTest {

    private fun sumWindowed(slices: Int = 10) =
        Sum().windowed(duration = 10.seconds, slices = slices)

    @Test
    fun `update within window is included in read`() {
        val w = sumWindowed()
        w.update(5.0, T3)
        assertEquals(5.0, w.read(T9).sum, DELTA)
    }

    @Test
    fun `update older than window is excluded`() {
        val w = sumWindowed()
        w.update(100.0, T0)
        w.update(5.0, T10)
        assertEquals(5.0, w.read(T11).sum, DELTA)
    }

    @Test
    fun `multiple updates in same slice accumulate`() {
        val w = sumWindowed()
        w.update(3.0, T3)
        w.update(4.0, T3)
        w.update(2.0, T3)
        assertEquals(9.0, w.read(T9).sum, DELTA)
    }

    @Test
    fun `updates in different slices all included when within window`() {
        val w = sumWindowed()
        w.update(1.0, T3)
        w.update(2.0, T9)
        assertEquals(3.0, w.read(T9).sum, DELTA)
    }

    @Test
    fun `all updates expire after full window elapses`() {
        val w = sumWindowed()
        w.update(99.0, T0)
        w.update(99.0, T3)
        w.update(99.0, T9)
        assertEquals(0.0, w.read(T20).sum, DELTA)
    }

    @Test
    fun `slot rotation evicts stale data from reused bucket`() {
        val w = sumWindowed()
        w.update(100.0, T0)
        w.update(7.0, T10)
        assertEquals(7.0, w.read(T11).sum, DELTA)
    }

    @Test
    fun `late out-of-order event is dropped`() {
        val w = sumWindowed()
        w.update(5.0, T10)
        w.update(99.0, T0)
        assertEquals(5.0, w.read(T11).sum, DELTA)
    }

    @Test
    fun `reset clears all buckets`() {
        val w = sumWindowed()
        w.update(10.0, T3)
        w.update(20.0, T9)
        w.reset()
        assertEquals(0.0, w.read(T9).sum, DELTA)
    }

    @Test
    fun `create produces fresh independent stat`() {
        val w1 = sumWindowed()
        w1.update(10.0, T3)
        val w2 = w1.create()
        w2.update(5.0, T3)
        assertEquals(10.0, w1.read(T9).sum, DELTA)
        assertEquals(5.0, w2.read(T9).sum, DELTA)
    }

    @Test
    fun `works with Mean stat`() {
        val w = Mean().windowed(duration = 10.seconds)
        w.update(4.0, T3)
        w.update(6.0, T9)
        assertEquals(5.0, w.read(T9).mean, DELTA)
    }

    @Test
    fun `single slice covers entire window`() {
        val w = Sum().windowed(duration = 10.seconds, slices = 1)
        w.update(3.0, T0)
        w.update(4.0, T3)
        assertEquals(7.0, w.read(T9).sum, DELTA)
    }

    @Test
    fun `weighted update is respected`() {
        val w = sumWindowed()
        w.update(1.0, T3, weight = 5.0)
        assertEquals(5.0, w.read(T9).sum, DELTA)
    }

    @Test
    fun `read excludes slots newer than the read timestamp`() {
        val w = sumWindowed()
        w.update(7.0, T10)
        assertEquals(0.0, w.read(T9).sum, DELTA)
    }

    @Test
    fun `update exactly at cutoff boundary is included`() {
        val w = sumWindowed()
        w.update(8.0, T0)
        assertEquals(8.0, w.read(T10).sum, DELTA)
    }

    @Test
    fun `paired windowed works with axis selection`() {
        val w = Sum().atX().windowed(duration = 10.seconds, slices = 10)
        w.update(2.0, 100.0, T3)
        w.update(3.0, 200.0, T9)
        assertEquals(5.0, w.read(T9).sum, DELTA)
    }

    @Test
    fun `vector windowed works with index selection`() {
        val w = Sum().atIndex(1).windowed(duration = 10.seconds, slices = 10)
        w.update(doubleArrayOf(1.0, 4.0), T3)
        w.update(doubleArrayOf(1.0, 6.0), T9)
        assertEquals(10.0, w.read(T9).sum, DELTA)
    }

    @Test
    fun `discrete windowed only counts in-window keys`() {
        val w = com.eignex.kumulant.stat.LinearCounting(bits = 4096)
            .windowed(duration = 10.seconds)
        w.update(1L, T0)         // expires before T11
        w.update(2L, T3)
        w.update(3L, T9)
        w.update(4L, T10)
        val seen = w.read(T11).estimate
        // Only 3 keys (2L, 3L, 4L) remain in the [T1, T11] window.
        assertTrue(seen in 2.0..4.0, "estimate=$seen")
    }

    @Test
    fun `windowed rejects invalid configuration`() {
        assertFailsWith<IllegalArgumentException> {
            Sum().windowed(duration = Duration.ZERO)
        }
        assertFailsWith<IllegalArgumentException> {
            Sum().windowed(duration = (-1).nanoseconds)
        }
        assertFailsWith<IllegalArgumentException> {
            Sum().windowed(duration = 10.seconds, slices = 0)
        }
        assertFailsWith<IllegalArgumentException> {
            Sum().windowed(duration = 1.nanoseconds, slices = 2)
        }
    }
}
