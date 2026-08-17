package com.eignex.kumulant.stat.change

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// ADWIN's window has to stay a suffix of the stream, and the bucket rows encode that ordering.
// Getting it wrong is invisible in the window length while the reported mean describes data the
// stream has moved past.
class AdwinWindowTest {

    @Test
    fun `the window reflects the current regime after two changes`() {
        val stat = AdwinStat()
        repeat(300) { stat.update(0.0) }
        repeat(300) { stat.update(10.0) }
        repeat(600) { stat.update(0.0) }

        val r = stat.read()

        // Every one of the last 600 observations was 0.0, so any window that is a suffix of the
        // stream must have mean 0.
        assertEquals(0.0, r.mean, 1e-9, "window of length ${r.windowLength} still holds stale data")
        assertEquals(0.0, r.variance, 1e-9)
    }

    @Test
    fun `a stable stream keeps its whole window and raises no alarm`() {
        val stat = AdwinStat()
        repeat(500) { stat.update(5.0) }

        val r = stat.read()

        assertEquals(500L, r.windowLength)
        assertEquals(5.0, r.mean, 1e-9)
        assertEquals(0L, r.changesDetected, "a constant stream has no change points")
    }

    @Test
    fun `a single sharp change is detected and the window follows it`() {
        val stat = AdwinStat()
        repeat(400) { stat.update(0.0) }
        repeat(400) { stat.update(100.0) }

        val r = stat.read()

        assertTrue(r.changesDetected > 0L, "a 0 to 100 jump must register as a change")
        assertEquals(100.0, r.mean, 1e-6, "the window should have followed the new regime")
        assertTrue(r.windowLength <= 400L, "the window must not span the change point")
    }

    @Test
    fun `the window never grows past the observation count`() {
        val stat = AdwinStat()
        repeat(1000) { stat.update(it.toDouble() % 7.0) }

        assertTrue(stat.read().windowLength <= 1000L)
    }
}
