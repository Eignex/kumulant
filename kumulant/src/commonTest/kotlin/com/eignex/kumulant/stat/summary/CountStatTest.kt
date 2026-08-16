package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.DELTA
import kotlin.test.Test
import kotlin.test.assertEquals

class CountStatTest {

    @Test
    fun `CountStat ignores incoming weights and counts updates`() {
        val count = CountStat()
        count.update(10.0, weight = 2.5)
        count.update(20.0, weight = 7.5)
        assertEquals(2.0, count.read().sum, DELTA)
    }
}
