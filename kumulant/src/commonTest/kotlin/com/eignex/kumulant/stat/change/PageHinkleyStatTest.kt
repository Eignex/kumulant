package com.eignex.kumulant.stat.change

import com.eignex.kumulant.DELTA
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class PageHinkleyStatTest {

    @Test
    fun `read before any update reports zero state and no alarm`() {
        val r = PageHinkleyStat().read()
        assertEquals(0L, r.count)
        assertFalse(r.alarm)
    }

    @Test
    fun `negative parameters are rejected`() {
        assertFailsWith<IllegalArgumentException> { PageHinkleyStat(delta = -0.1) }
        assertFailsWith<IllegalArgumentException> { PageHinkleyStat(threshold = -0.1) }
    }

    @Test
    fun `stationary stream does not trip the alarm`() {
        val s = PageHinkleyStat(delta = 0.005, threshold = 50.0)
        // Symmetric alternating values: mean stays near zero, no sustained drift.
        for (idx in 0 until 200) s.update(if (idx % 2 == 0) 1.0 else -1.0)
        assertFalse(s.read().alarm)
    }

    @Test
    fun `sustained positive shift trips alarmUp`() {
        val s = PageHinkleyStat(delta = 0.005, threshold = 5.0)
        repeat(50) { s.update(0.0) }
        repeat(50) { s.update(1.0) }
        val r = s.read()
        assertTrue(r.alarmUp, "cumPos=${r.cumulativePositive} minPos=${r.minPositive}")
    }

    @Test
    fun `sustained negative shift trips alarmDown`() {
        val s = PageHinkleyStat(delta = 0.005, threshold = 5.0)
        repeat(50) { s.update(0.0) }
        repeat(50) { s.update(-1.0) }
        val r = s.read()
        assertTrue(r.alarmDown)
    }

    @Test
    fun `running mean tracks the underlying mean`() {
        val s = PageHinkleyStat()
        for (i in 1..10) s.update(i.toDouble())
        assertEquals(5.5, s.read().mean, DELTA)
    }

    @Test
    fun `reset clears state`() {
        val s = PageHinkleyStat(threshold = 3.0).apply {
            repeat(50) { update(1.0) }
        }
        s.reset()
        val r = s.read()
        assertEquals(0L, r.count)
        assertEquals(0.0, r.mean, DELTA)
        assertFalse(r.alarm)
    }

    @Test
    fun `create produces an independent stat`() {
        val tpl = PageHinkleyStat().apply { repeat(5) { update(1.0) } }
        val fresh = tpl.create()
        assertEquals(0L, fresh.read().count)
        assertTrue(tpl.read().count > 0)
    }
}

class PageHinkleyMergeAlarmTest {

    @Test
    fun `merging two traces that saw no upward drift does not raise one`() {
        val downwardShift = PageHinkleyStat()
        repeat(60) { downwardShift.update(0.0) }
        repeat(60) { downwardShift.update(-5.0) }
        val flat = PageHinkleyStat()
        repeat(120) { flat.update(0.0) }

        assertFalse(downwardShift.read().alarmUp, "the downward-shift trace already alarmed up")
        assertFalse(flat.read().alarmUp, "the flat trace already alarmed up")

        downwardShift.merge(flat.read())
        assertFalse(downwardShift.read().alarmUp, "merging two quiet traces manufactured an upward alarm")
    }

    @Test
    fun `merging two traces that saw no downward drift does not raise one`() {
        val upwardShift = PageHinkleyStat()
        repeat(60) { upwardShift.update(0.0) }
        repeat(60) { upwardShift.update(5.0) }
        val flat = PageHinkleyStat()
        repeat(120) { flat.update(0.0) }

        assertFalse(upwardShift.read().alarmDown, "the upward-shift trace already alarmed down")
        assertFalse(flat.read().alarmDown, "the flat trace already alarmed down")

        upwardShift.merge(flat.read())
        assertFalse(upwardShift.read().alarmDown, "merging two quiet traces manufactured a downward alarm")
    }
}
