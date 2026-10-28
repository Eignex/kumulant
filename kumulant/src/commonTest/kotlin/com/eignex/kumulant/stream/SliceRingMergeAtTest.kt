package com.eignex.kumulant.stream

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stat.summary.SumStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

class SliceRingMergeAtTest {

    private class RecordingSumStat : SeriesStat<SumResult> {
        override val concurrency = Concurrency.None

        override fun update(value: Double, timestampNanos: Long, weight: Double) = Unit

        override fun merge(values: SumResult) = Unit

        override fun reset() = Unit

        override fun read(timestampNanos: Long) = SumResult(0.0)

        override fun create(concurrency: Concurrency?) = RecordingSumStat()
    }

    private fun newRing() = SliceRing<SumResult, SumStat>(
        windowDuration = 10.seconds,
        slices = 10,
        concurrency = Concurrency.None,
    ) { c -> SumStat(c) }

    @Test
    fun `merges within one slice sum into the same slot`() {
        val ring = newRing()
        val sliceNanos = 1_000_000_000L
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
        val newerStart = 10 * sliceNanos
        ring.mergeAt(newerStart + 100, SumResult(5.0))
        ring.mergeAt(50, SumResult(99.0))
        var total = 0.0
        ring.forEachActive(newerStart + 500) { total += it.read().sum }
        assertEquals(5.0, total, DELTA)
    }

    @Test
    fun `merge resumes after gaps longer than the window`() {
        val ring = newRing()
        ring.mergeAt(0L, SumResult(1.0))
        ring.mergeAt(11_000_000_000L, SumResult(7.0))
        var total = 0.0
        ring.forEachActive(11_000_000_000L) { total += it.read().sum }

        assertEquals(7.0, total, DELTA)
    }
}
