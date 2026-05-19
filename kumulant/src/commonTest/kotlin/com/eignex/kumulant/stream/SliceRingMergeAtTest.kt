package com.eignex.kumulant.stream

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stat.summary.SumStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

private const val DELTA = 1e-12

class SliceRingMergeAtTest {

    private fun newRing() = SliceRing<SumResult, SumStat>(
        windowDuration = 10.seconds,
        slices = 10,
        concurrency = Concurrency.None,
    ) { c -> SumStat(c) }

    /**
     * Two merges into the same slice land in the same slot and sum together.
     * Sanity check that mergeAt routes by timestamp.
     */
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

    /**
     * If a later thread has already rotated the bucket past expectedStart (so
     * bucketRef.load() returns a slot whose startNanos > expectedStart), mergeAt
     * must not merge into that future slot. The pre-fix code's fallback
     * `bucketRef.load()` would do exactly that. We simulate the post-rotation
     * state by calling mergeAt on a newer timestamp first to install the future
     * slot, then mergeAt on an older timestamp that targets the same bucket index.
     */
    @Test
    fun `merge at a stale timestamp does not contaminate a newer slot`() {
        val ring = newRing()
        val sliceNanos = 1_000_000_000L
        // 10 buckets, so slice index 10 collides with index 0 in the ring.
        // Install slot for slice 10 first (start = 10 * sliceNanos).
        val newerStart = 10 * sliceNanos
        ring.mergeAt(newerStart + 100, SumResult(5.0))
        // Now try to merge at a timestamp belonging to slice 0 (start = 0).
        // This shares the bucket with slice 10. The slot currently in that
        // bucket has startNanos = 10*sliceNanos, which is > expectedStart = 0.
        // mergeAt must skip the merge rather than overwriting the newer slot.
        ring.mergeAt(50, SumResult(99.0))
        // Read the newer slice's contents - must still be 5.0, not 5+99.
        var total = 0.0
        ring.forEachActive(newerStart + 500) { total += it.read().sum }
        assertEquals(5.0, total, DELTA)
    }
}
