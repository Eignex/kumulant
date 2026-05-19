package com.eignex.kumulant.stat.summary

import kotlin.test.Test
import kotlin.test.assertEquals

private const val DELTA = 1e-12

class TotalWeightsStatTest {

    @Test
    fun `TotalWeightsStat sums incoming weights`() {
        val count = TotalWeightsStat()
        count.update(10.0, weight = 2.5)
        count.update(20.0, weight = 7.5)
        assertEquals(10.0, count.read().sum, DELTA)
    }
}
