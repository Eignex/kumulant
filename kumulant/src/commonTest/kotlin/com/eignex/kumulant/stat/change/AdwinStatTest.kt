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
        assertTrue(r.windowLength > 1_500L, "windowLength=${r.windowLength}")
    }

    @Test
    fun `sudden mean shift triggers a change and shrinks the window`() {
        val s = AdwinStat()
        val rng = Random(7)
        repeat(500) { s.update(rng.nextDouble() * 0.05) }
        repeat(500) { s.update(0.95 + rng.nextDouble() * 0.05) }
        val r = s.read()
        assertTrue(r.changesDetected > 0L, "no change detected")
        assertTrue(r.windowLength < 1_000L, "windowLength=${r.windowLength}")
        assertTrue(r.mean > 0.2, "post-shift mean=${r.mean}")
    }

    @Test
    fun `mean and variance reflect the current window`() {
        val s = AdwinStat()
        val xs = doubleArrayOf(0.1, 0.2, 0.3, 0.4, 0.5)
        for (x in xs) s.update(x)
        val r = s.read()
        assertEquals(0L, r.changesDetected)
        assertEquals(0.3, r.mean, DELTA)
    }

    @Test
    fun `merge adds the remote change count without replacing the local window`() {
        val stat = AdwinStat().apply { update(2.0) }
        val before = stat.read()
        val remote = AdwinStat().apply { update(10.0) }.read().copy(changesDetected = 3L)

        stat.merge(remote)

        assertEquals(before.copy(changesDetected = 3L), stat.read())
    }
}
