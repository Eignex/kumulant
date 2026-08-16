package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.DELTA
import kotlin.test.Test
import kotlin.test.assertEquals

class TotalWeightsStatTest {

    @Test
    fun `TotalWeightsStat sums incoming weights`() {
        val count = TotalWeightsStat()
        count.update(10.0, weight = 2.5)
        count.update(20.0, weight = 7.5)
        assertEquals(10.0, count.read().sum, DELTA)
    }
}
