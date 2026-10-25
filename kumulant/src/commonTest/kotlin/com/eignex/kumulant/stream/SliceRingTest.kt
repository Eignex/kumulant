package com.eignex.kumulant.stream

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stat.summary.SumStat
import kotlin.test.Test
import kotlin.test.assertNotSame
import kotlin.test.assertSame
import kotlin.time.Duration.Companion.seconds

class SliceRingTest {

    private fun newRing() = SliceRing<SumResult, SumStat>(
        windowDuration = 10.seconds,
        slices = 10,
        concurrency = Concurrency.None,
    ) { c -> SumStat(c) }

    @Test
    fun `slotFor maps negative timestamps in different slices to different slots`() {
        val ring = newRing()
        val slotMinusOne = ring.slotFor(-1L)!!
        val slotZero = ring.slotFor(0L)!!
        assertNotSame(slotMinusOne, slotZero)
    }

    @Test
    fun `slotFor returns same slot for two timestamps in the same negative slice`() {
        val ring = newRing()
        val sliceNanos = 1_000_000_000L
        val a = ring.slotFor(-sliceNanos - 5)!!
        val b = ring.slotFor(-sliceNanos - 200)!!
        assertSame(a, b)
    }
}
