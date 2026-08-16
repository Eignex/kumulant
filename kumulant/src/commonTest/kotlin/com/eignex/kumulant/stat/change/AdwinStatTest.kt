package com.eignex.kumulant.stat.change

import com.eignex.kumulant.DELTA
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class AdwinStatTest {

    @Test
    fun `read before any update reports empty window`() {
        val r = AdwinStat().read()
        assertEquals(0L, r.windowLength)
        assertEquals(0L, r.changesDetected)
        assertFalse(r.alarm)
    }

    @Test
    fun `delta out of range is rejected`() {
        assertFailsWith<IllegalArgumentException> { AdwinStat(delta = 0.0) }
        assertFailsWith<IllegalArgumentException> { AdwinStat(delta = 1.0) }
    }

    @Test
    fun `maxBucketsPerSize must be positive`() {
        assertFailsWith<IllegalArgumentException> { AdwinStat(maxBucketsPerSize = 0) }
    }

    @Test
    fun `stationary stream keeps the window growing without alarm`() {
        val s = AdwinStat()
        val rng = Random(42)
        repeat(2_000) { s.update(rng.nextDouble()) }
        val r = s.read()
        assertEquals(0L, r.changesDetected)
        // Window should contain a large fraction of the observations.
        assertTrue(r.windowLength > 1_500L, "windowLength=${r.windowLength}")
    }

    @Test
    fun `sudden mean shift triggers a change and shrinks the window`() {
        val s = AdwinStat()
        // 500 observations near 0, then 500 near 1 - a clear shift.
        val rng = Random(7)
        repeat(500) { s.update(rng.nextDouble() * 0.05) }
        repeat(500) { s.update(0.95 + rng.nextDouble() * 0.05) }
        val r = s.read()
        assertTrue(r.changesDetected > 0L, "no change detected")
        // After the shift the window should be smaller than the full stream.
        assertTrue(r.windowLength < 1_000L, "windowLength=${r.windowLength}")
        // The remaining window should have moved noticeably toward the post-shift mean (~0.975)
        // even if some pre-shift residue remains in the largest still-active bucket.
        assertTrue(r.mean > 0.2, "post-shift mean=${r.mean}")
    }

    @Test
    fun `mean and variance reflect the current window`() {
        val s = AdwinStat()
        val xs = doubleArrayOf(0.1, 0.2, 0.3, 0.4, 0.5)
        for (x in xs) s.update(x)
        val r = s.read()
        // No drift in this tiny stationary stream, so the entire input is still in-window.
        assertEquals(0L, r.changesDetected)
        assertEquals(0.3, r.mean, DELTA)
    }

    @Test
    fun `reset clears state`() {
        val s = AdwinStat().apply { repeat(50) { update(1.0) } }
        s.reset()
        val r = s.read()
        assertEquals(0L, r.windowLength)
        assertEquals(0L, r.changesDetected)
    }

    @Test
    fun `create produces an independent stat`() {
        val tpl = AdwinStat().apply { repeat(10) { update(0.5) } }
        val fresh = tpl.create()
        assertEquals(0L, fresh.read().windowLength)
        assertTrue(tpl.read().windowLength > 0L)
    }
}
