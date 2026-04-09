package com.eignex.kumulant.stat

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

private const val DELTA = 1e-12

/**
 * Timestamps for windowed tests. Window = 10s, slices = 10, so each slice = 1s.
 * Bucket index = (timestamp_seconds) % 10.
 */
private const val T0 = 0L                   // slice 0
private const val T3 = 3_000_000_000L       // slice 3
private const val T9 = 9_000_000_000L       // slice 9
private const val T10 = 10_000_000_000L     // slice 0 again (rotates bucket)
private const val T11 = 11_000_000_000L     // slice 1 again (rotates bucket)
private const val T20 = 20_000_000_000L     // all original slices have expired

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
        w.update(100.0, T0)   // at T10 this is exactly 10s old — on the boundary
        w.update(5.0, T10)    // bucket rotation evicts T0's data from slot 0
        // read at T11: cutoff = T11 - 10s = T1, so T0 is excluded anyway
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
        // read at T9: cutoff = T9 - 10s = -1s, both T3 and T9 are within window
        assertEquals(3.0, w.read(T9).sum, DELTA)
    }

    @Test
    fun `all updates expire after full window elapses`() {
        val w = sumWindowed()
        w.update(99.0, T0)
        w.update(99.0, T3)
        w.update(99.0, T9)
        // At T20, cutoff = T20 - 10s = T10; all updates at T0/T3/T9 are older
        assertEquals(0.0, w.read(T20).sum, DELTA)
    }

    @Test
    fun `slot rotation evicts stale data from reused bucket`() {
        val w = sumWindowed()
        w.update(100.0, T0)   // bucket index 0 at T0
        w.update(7.0, T10)    // bucket index 0 at T10 → evicts T0 data
        // T0 data (100) is gone; T10 data (7) is within window at T11
        assertEquals(7.0, w.read(T11).sum, DELTA)
    }

    @Test
    fun `late out-of-order event is dropped`() {
        val w = sumWindowed()
        w.update(5.0, T10)    // bucket 0 now holds data at T10
        w.update(99.0, T0)    // T0 < T10 for same bucket → dropped
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
    fun `copy is independent`() {
        val w1 = sumWindowed()
        w1.update(10.0, T3)
        val w2 = w1.create()
        w2.update(5.0, T3)
        // w1 should not see w2's update (copy is fresh)
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
        // At T9 both are within the 10s window
        assertEquals(7.0, w.read(T9).sum, DELTA)
    }

    @Test
    fun `weighted update is respected`() {
        val w = sumWindowed()
        w.update(1.0, T3, weight = 5.0)
        assertEquals(5.0, w.read(T9).sum, DELTA)
    }
}
