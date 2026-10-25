package com.eignex.kumulant.stat.change

import kotlin.test.Test
import kotlin.test.assertEquals

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
}
