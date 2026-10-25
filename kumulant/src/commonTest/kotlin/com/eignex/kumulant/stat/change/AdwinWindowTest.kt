package com.eignex.kumulant.stat.change

import kotlin.test.Test
import kotlin.test.assertEquals

class AdwinWindowTest {

    @Test
    fun `the window reflects the current regime after two changes`() {
        val stat = AdwinStat()
        repeat(300) { stat.update(0.0) }
        repeat(300) { stat.update(10.0) }
        repeat(600) { stat.update(0.0) }

        val r = stat.read()

        assertEquals(0.0, r.mean, 1e-9, "window of length ${r.windowLength} still holds stale data")
        assertEquals(0.0, r.variance, 1e-9)
    }
}
