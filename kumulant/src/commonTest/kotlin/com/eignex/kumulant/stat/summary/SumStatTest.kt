package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.DELTA
import kotlin.test.Test
import kotlin.test.assertEquals

class SumStatTest {
    @Test
    fun `create produces fresh independent stat`() {
        val s1 = SumStat().apply { update(10.0) }
        val s2 = s1.create()
        s1.update(5.0)
        assertEquals(15.0, s1.read().sum, DELTA)
        assertEquals(0.0, s2.read().sum, DELTA)
    }

    @Test
    fun `test extreme values`() {
        val sum = SumStat()
        sum.update(1e15, 1.0)
        sum.update(1.0, 1.0)
        assertEquals(1000000000000001.0, sum.read().sum, 0.1)
    }

    @Test
    fun `test negative weights and values`() {
        val sum = SumStat()
        sum.update(-10.0, 1.0)
        sum.update(10.0, -1.0)
        assertEquals(-20.0, sum.read().sum, DELTA)
    }

    @Test
    fun `test merge logic`() {
        val s1 = SumStat().apply { update(10.0, 1.0) }
        val s2 = SumStat().apply { update(20.0, 1.0) }
        s1.merge(s2.read())
        assertEquals(30.0, s1.read().sum, DELTA)
    }

    @Test
    fun `test reset`() {
        val sum = SumStat()
        sum.update(100.0)
        sum.reset()
        assertEquals(0.0, sum.read().sum, DELTA)
    }
}
