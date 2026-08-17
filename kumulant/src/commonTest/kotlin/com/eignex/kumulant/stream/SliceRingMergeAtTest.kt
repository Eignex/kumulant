package com.eignex.kumulant.stream

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stat.summary.SumStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

class SliceRingMergeAtTest {

    private fun newRing() = SliceRing<SumResult, SumStat>(
        windowDuration = 10.seconds,
        slices = 10,
        concurrency = Concurrency.None,
    ) { c -> SumStat(c) }

    @Test
    fun `merges within one slice sum into the same slot`() {
        val ring = newRing()
        val sliceNanos = 1_000_000_000L // 10s / 10 slices
        ring.mergeAt(sliceNanos + 100, SumResult(3.0))
        ring.mergeAt(sliceNanos + 800, SumResult(4.0))
        var total = 0.0
        ring.forEachActive(sliceNanos + 1000) { total += it.read().sum }
        assertEquals(7.0, total, DELTA)
    }

    @Test
    fun `merge at a stale timestamp does not contaminate a newer slot`() {
        val ring = newRing()
        val sliceNanos = 1_000_000_000L
        // 10 buckets, so slice index 10 collides with index 0 in the ring. Installing slice 10 first
        // simulates a bucket another thread has already rotated past expectedStart.
        val newerStart = 10 * sliceNanos
        ring.mergeAt(newerStart + 100, SumResult(5.0))
        // Slice 0 shares that bucket, whose slot has startNanos = 10 * sliceNanos > expectedStart = 0,
        // so mergeAt must skip the merge rather than overwrite the newer slot.
        ring.mergeAt(50, SumResult(99.0))
        var total = 0.0
        ring.forEachActive(newerStart + 500) { total += it.read().sum }
        assertEquals(5.0, total, DELTA)
    }
}
