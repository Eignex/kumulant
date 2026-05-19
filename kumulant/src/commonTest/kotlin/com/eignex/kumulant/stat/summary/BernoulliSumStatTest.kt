package com.eignex.kumulant.stat.summary

import kotlin.test.Test
import kotlin.test.assertEquals

private const val DELTA = 1e-12

class BernoulliSumStatTest {

    @Test
    fun `unit-weight binary updates count successes and trials`() {
        val stat = BernoulliSumStat().apply {
            update(value = 1.0, timestampNanos = 0L, weight = 1.0)
            update(value = 0.0, timestampNanos = 0L, weight = 1.0)
            update(value = 1.0, timestampNanos = 0L, weight = 1.0)
        }
        val r = stat.read(0L)
        assertEquals(2.0, r.successes, DELTA)
        assertEquals(3.0, r.trials, DELTA)
    }

    @Test
    fun `weighted updates accumulate fractional successes`() {
        val stat = BernoulliSumStat().apply {
            update(value = 1.0, timestampNanos = 0L, weight = 0.5)
            update(value = 1.0, timestampNanos = 0L, weight = 1.5)
        }
        val r = stat.read(0L)
        assertEquals(2.0, r.successes, DELTA)
        assertEquals(2.0, r.trials, DELTA)
    }

    @Test
    fun `zero weight is a noop`() {
        val stat = BernoulliSumStat().apply { update(1.0, 0L, 0.0) }
        val r = stat.read(0L)
        assertEquals(0.0, r.successes, DELTA)
        assertEquals(0.0, r.trials, DELTA)
    }

    @Test
    fun `merge adds component-wise`() {
        val a = BernoulliSumStat().apply { update(1.0, 0L, 1.0) }
        val b = BernoulliSumStat().apply {
            update(0.0, 0L, 1.0)
            update(1.0, 0L, 1.0)
        }
        a.merge(b.read(0L))
        val r = a.read(0L)
        assertEquals(2.0, r.successes, DELTA)
        assertEquals(3.0, r.trials, DELTA)
    }

    @Test
    fun `reset clears state`() {
        val stat = BernoulliSumStat().apply {
            update(1.0, 0L, 1.0)
            reset()
        }
        val r = stat.read(0L)
        assertEquals(0.0, r.successes, DELTA)
        assertEquals(0.0, r.trials, DELTA)
    }
}
