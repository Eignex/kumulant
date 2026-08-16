package com.eignex.kumulant.stat.change

import com.eignex.kumulant.DELTA
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CusumStatTest {

    @Test
    fun `read before any update reports zero cusums and no alarm`() {
        val r = CusumStat().read()
        assertEquals(0.0, r.cusumPositive, DELTA)
        assertEquals(0.0, r.cusumNegative, DELTA)
        assertFalse(r.alarm)
    }

    @Test
    fun `negative parameters are rejected`() {
        assertFailsWith<IllegalArgumentException> { CusumStat(referenceValue = -0.1) }
        assertFailsWith<IllegalArgumentException> { CusumStat(threshold = -0.1) }
    }

    @Test
    fun `in-control samples within reference value stay near zero`() {
        val s = CusumStat(target = 0.0, referenceValue = 0.5, threshold = 5.0)
        for (v in listOf(0.1, -0.2, 0.3, -0.4, 0.2, -0.1)) s.update(v)
        val r = s.read()
        assertFalse(r.alarm)
        assertEquals(0.0, r.cusumPositive, DELTA)
    }

    @Test
    fun `sustained positive shift trips alarmUp`() {
        val s = CusumStat(target = 0.0, referenceValue = 0.5, threshold = 3.0)
        repeat(10) { s.update(2.0) } // strong upward shift
        val r = s.read()
        assertTrue(r.alarmUp)
        assertFalse(r.alarmDown)
        assertTrue(r.cusumPositive > r.threshold)
    }

    @Test
    fun `sustained negative shift trips alarmDown`() {
        val s = CusumStat(target = 0.0, referenceValue = 0.5, threshold = 3.0)
        repeat(10) { s.update(-2.0) }
        val r = s.read()
        assertTrue(r.alarmDown)
        assertFalse(r.alarmUp)
        assertTrue(-r.cusumNegative > r.threshold)
    }

    @Test
    fun `positive cusum cannot drop below zero`() {
        val s = CusumStat(target = 0.0, referenceValue = 0.5, threshold = 5.0)
        s.update(value = 2.0) // pushes cusumPositive to 1.5
        s.update(value = -10.0) // would drive cusumPositive negative
        val r = s.read()
        assertEquals(0.0, r.cusumPositive, DELTA)
    }

    @Test
    fun `reset clears state`() {
        val s = CusumStat(threshold = 3.0).apply {
            repeat(10) { update(2.0) }
        }
        s.reset()
        val r = s.read()
        assertEquals(0.0, r.cusumPositive, DELTA)
        assertEquals(0.0, r.cusumNegative, DELTA)
        assertFalse(r.alarm)
    }

    @Test
    fun `create produces an independent stat`() {
        val tpl = CusumStat().apply { repeat(5) { update(2.0) } }
        val fresh = tpl.create()
        assertEquals(0.0, fresh.read().cusumPositive, DELTA)
        assertTrue(tpl.read().cusumPositive > 0.0)
    }
}
