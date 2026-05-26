package com.eignex.kumulant.stat.score

import kotlin.test.Test
import kotlin.test.assertEquals

private const val DELTA = 1e-9

class AccuracyStatTest {

    @Test
    fun `accuracy is the fraction of matching class labels`() {
        val s = AccuracyStat()
        s.update(1.0, 1.0)
        s.update(0.0, 0.0)
        s.update(1.0, 0.0)
        s.update(2.0, 2.0)
        s.update(0.0, 1.0)
        // 3 out of 5 match.
        assertEquals(3.0 / 5.0, s.read().mean, DELTA)
    }

    @Test
    fun `class labels round trip through toLong so integer doubles compare safely`() {
        val s = AccuracyStat()
        s.update(7.0, 7.0)
        s.update(2.0, 3.0)
        assertEquals(0.5, s.read().mean, DELTA)
    }
}
