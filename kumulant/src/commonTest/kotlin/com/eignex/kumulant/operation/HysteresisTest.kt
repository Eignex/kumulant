package com.eignex.kumulant.operation

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.stat.summary.SumStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class HysteresisTest {

    @Test
    fun `transitions to high when value rises above high`() {
        val stat = SumStat().hysteresis(low = 1.0, high = 5.0)
        listOf(0.0, 2.0, 3.0, 6.0, 4.0).forEach { stat.update(it) }
        assertEquals(2.0, stat.read().sum, DELTA)
    }

    @Test
    fun `transitions back to low only after value falls below low`() {
        val stat = SumStat().hysteresis(low = 1.0, high = 5.0)
        listOf(10.0, 3.0, 2.0, 0.5, 3.0).forEach { stat.update(it) }
        assertEquals(3.0, stat.read().sum, DELTA)
    }

    @Test
    fun `deadband values keep the current state`() {
        val stat = SumStat().hysteresis(low = 1.0, high = 5.0)
        listOf(6.0, 3.0, 2.0, 4.0, 3.0).forEach { stat.update(it) }
        assertEquals(5.0, stat.read().sum, DELTA)
    }

    @Test
    fun `initial value in deadband starts in low state`() {
        val stat = SumStat().hysteresis(low = 1.0, high = 5.0)
        listOf(3.0, 4.0, 2.0).forEach { stat.update(it) }
        assertEquals(0.0, stat.read().sum, DELTA)
    }

    @Test
    fun `low greater than high is rejected`() {
        assertFailsWith<IllegalArgumentException> {
            SumStat().hysteresis(low = 5.0, high = 1.0)
        }
    }

    @Test
    fun `low equal to high is allowed and behaves like a single threshold`() {
        val stat = SumStat().hysteresis(low = 1.0, high = 1.0)
        listOf(0.5, 1.5, 1.0, 0.5).forEach { stat.update(it) }
        assertEquals(2.0, stat.read().sum, DELTA)
    }

    @Test
    fun `reset clears state`() {
        val stat = SumStat().hysteresis(low = 1.0, high = 5.0).apply {
            update(10.0)
            update(4.0)
        }
        stat.reset()
        stat.update(0.0)
        assertEquals(0.0, stat.read().sum, DELTA)
    }

    @Test
    fun `create produces an independent stat`() {
        val template = SumStat().hysteresis(low = 1.0, high = 5.0).apply { update(10.0) }
        val fresh = template.create()
        fresh.update(0.0)
        assertEquals(1.0, template.read().sum, DELTA)
        assertEquals(0.0, fresh.read().sum, DELTA)
    }
}
