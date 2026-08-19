package com.eignex.kumulant.operation

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.stat.summary.SumStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

private const val NS_PER_SEC = 1_000_000_000L

class ShiftsTest {

    @Test
    fun `lag of 1 forwards previous value and suppresses the first update`() {
        val stat = SumStat().lag(1)
        stat.update(10.0)
        stat.update(20.0)
        stat.update(30.0)
        assertEquals(10.0 + 20.0, stat.read().sum, DELTA)
    }

    @Test
    fun `lag of k forwards value from k updates ago`() {
        val stat = SumStat().lag(3)
        listOf(1.0, 2.0, 3.0, 4.0, 5.0).forEach { stat.update(it) }
        assertEquals(1.0 + 2.0, stat.read().sum, DELTA)
    }

    @Test
    fun `lag of k suppresses the first k updates entirely`() {
        val stat = SumStat().lag(3)
        stat.update(1.0)
        stat.update(2.0)
        stat.update(3.0)
        assertEquals(0.0, stat.read().sum, DELTA)
    }

    @Test
    fun `diff forwards first difference and suppresses the first update`() {
        val stat = SumStat().diff()
        listOf(10.0, 13.0, 11.0, 15.0).forEach { stat.update(it) }
        assertEquals((13.0 - 10.0) + (11.0 - 13.0) + (15.0 - 11.0), stat.read().sum, DELTA)
    }

    @Test
    fun `diff of k computes k-th difference`() {
        val stat = SumStat().diff(2)
        listOf(1.0, 3.0, 6.0, 10.0).forEach { stat.update(it) }
        assertEquals((6.0 - 1.0) + (10.0 - 3.0), stat.read().sum, DELTA)
    }

    @Test
    fun `derivative emits per-second rate of change`() {
        val stat = SumStat().derivative()
        stat.update(value = 0.0, timestampNanos = 0L)
        stat.update(value = 10.0, timestampNanos = 1L * NS_PER_SEC)
        stat.update(value = 30.0, timestampNanos = 3L * NS_PER_SEC)
        assertEquals(10.0 + 10.0, stat.read().sum, DELTA)
    }

    @Test
    fun `derivative drops updates with coincident timestamps`() {
        val stat = SumStat().derivative()
        stat.update(value = 0.0, timestampNanos = 5L)
        stat.update(value = 5.0, timestampNanos = 5L)
        stat.update(value = 7.0, timestampNanos = 5L + NS_PER_SEC)
        assertEquals((7.0 - 5.0) / 1.0, stat.read().sum, DELTA)
    }

    @Test
    fun `reset clears the ring`() {
        val stat = SumStat().lag(2)
        stat.update(1.0)
        stat.update(2.0)
        stat.update(3.0)
        stat.reset()
        stat.update(100.0)
        stat.update(200.0)
        stat.update(300.0)
        assertEquals(100.0, stat.read().sum, DELTA)
    }

    @Test
    fun `create produces an independent stat`() {
        val template = SumStat().lag(1)
        template.update(1.0)
        template.update(2.0)
        val fresh = template.create()
        fresh.update(100.0)
        fresh.update(200.0)
        assertEquals(1.0, template.read().sum, DELTA)
        assertEquals(100.0, fresh.read().sum, DELTA)
    }

    @Test
    fun `lag and diff reject k less than 1`() {
        assertFailsWith<IllegalArgumentException> { SumStat().lag(0) }
        assertFailsWith<IllegalArgumentException> { SumStat().diff(0) }
    }

    @Test
    fun `derivative skips an out-of-order observation and keeps its reference point`() {
        val stat = SumStat().derivative()
        stat.update(value = 10.0, timestampNanos = 100L)
        stat.update(value = 20.0, timestampNanos = 300L)
        stat.update(value = 30.0, timestampNanos = 200L)
        stat.update(value = 40.0, timestampNanos = 400L)
        // 5e7 from the second update, then 2e8 measured from (20.0, 300) rather than the skipped one.
        assertEquals(2.5e8, stat.read().sum, DELTA)
    }
}
