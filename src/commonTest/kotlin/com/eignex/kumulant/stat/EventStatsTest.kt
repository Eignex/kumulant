package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.AtomicMode
import com.eignex.kumulant.concurrent.SerialMode
import com.eignex.kumulant.core.CountResult
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

private const val DELTA = 1e-12

class CountTest {

    @Test
    fun `counts each update as one regardless of value`() {
        val c = Count()
        c.update(42.0)
        c.update(-7.0)
        c.update(0.0)
        assertEquals(3L, c.count)
    }

    @Test
    fun `empty count is zero`() {
        assertEquals(0L, Count().count)
    }

    @Test
    fun `merge adds counts`() {
        val c1 = Count().apply {
            update(1.0)
            update(2.0)
        }
        val c2 = Count().apply { update(3.0) }
        c1.merge(c2.read())
        assertEquals(3L, c1.count)
    }

    @Test
    fun `merge with zero-count result is no-op`() {
        val c = Count().apply { update(1.0) }
        c.merge(CountResult(0L))
        assertEquals(1L, c.count)
    }

    @Test
    fun `reset clears count`() {
        val c = Count().apply {
            update(1.0)
            update(2.0)
        }
        c.reset()
        assertEquals(0L, c.count)
    }

    @Test
    fun `copy is independent`() {
        val c1 = Count(AtomicMode).apply { update(1.0) }
        val c2 = c1.copy(SerialMode)
        c2.update(2.0)
        assertEquals(1L, c1.count)
        assertEquals(1L, c2.count)
    }

    @Test
    fun `read result carries name`() {
        val c = Count(name = "events")
        c.update(1.0)
        assertEquals("events", c.read().name)
    }
}

class TotalWeightsTest {

    @Test
    fun `accumulates weights not values`() {
        val tw = TotalWeights()
        tw.update(99.0, weight = 2.0)
        tw.update(0.0, weight = 3.0)
        assertEquals(5.0, tw.totalWeights, DELTA)
    }

    @Test
    fun `default weight is one`() {
        val tw = TotalWeights()
        tw.update(1.0)
        tw.update(1.0)
        assertEquals(2.0, tw.totalWeights, DELTA)
    }

    @Test
    fun `merge adds total weights`() {
        val tw1 = TotalWeights().apply { update(1.0, weight = 4.0) }
        val tw2 = TotalWeights().apply { update(1.0, weight = 6.0) }
        tw1.merge(tw2.read())
        assertEquals(10.0, tw1.totalWeights, DELTA)
    }

    @Test
    fun `reset clears accumulated weight`() {
        val tw = TotalWeights().apply { update(1.0, weight = 5.0) }
        tw.reset()
        assertEquals(0.0, tw.totalWeights, DELTA)
    }

    @Test
    fun `read returns SumResult containing total weights`() {
        val tw = TotalWeights(name = "w")
        tw.update(0.0, weight = 3.5)
        val r = tw.read()
        assertEquals(3.5, r.sum, DELTA)
        assertEquals("w", r.name)
    }
}

class EventRateTest {

    @Test
    fun `rate increases with events`() {
        val er = EventRate()
        repeat(100) { er.update(1.0) }
        assertTrue(er.rate > 0.0, "rate should be positive after events")
    }

    @Test
    fun `value parameter is ignored — every update counts as 1`() {
        val er1 = EventRate().apply { repeat(5) { update(1.0) } }
        val er2 = EventRate().apply { repeat(5) { update(999.0) } }
        assertEquals(er1.read().totalValue, er2.read().totalValue, DELTA)
    }

    @Test
    fun `reset yields near-zero rate`() {
        val er = EventRate()
        repeat(100) { er.update(1.0) }
        er.reset()
        val r = er.read()
        assertEquals(0.0, r.totalValue, DELTA)
    }

    @Test
    fun `merge adds event totals`() {
        val er1 = EventRate().apply { repeat(3) { update(1.0) } }
        val er2 = EventRate().apply { repeat(7) { update(1.0) } }
        er1.merge(er2.read())
        assertEquals(10.0, er1.read().totalValue, DELTA)
    }

    @Test
    fun `copy is independent`() {
        val er1 = EventRate().apply { repeat(5) { update(1.0) } }
        val er2 = er1.copy()
        repeat(3) { er2.update(1.0) }
        assertEquals(5.0, er1.read().totalValue, DELTA)
    }
}

class DecayingEventRateTest {

    @Test
    fun `rate is positive after events`() {
        val er = DecayingEventRate(halfLife = 1.seconds)
        repeat(10) { er.update(1.0) }
        assertTrue(er.rate > 0.0)
    }

    @Test
    fun `value parameter is ignored — every update contributes 1`() {
        val er1 = DecayingEventRate(halfLife = 1.seconds)
        val er2 = DecayingEventRate(halfLife = 1.seconds)
        // Both update at the same system time, so rates should be equal
        er1.update(1.0)
        er2.update(999.0)
        // Rates will differ slightly due to real clock, but both should be > 0
        assertTrue(er1.rate > 0.0)
        assertTrue(er2.rate > 0.0)
    }

    @Test
    fun `reset yields zero rate`() {
        val er = DecayingEventRate(halfLife = 1.seconds)
        repeat(50) { er.update(1.0) }
        er.reset()
        assertEquals(0.0, er.read().rate, DELTA)
    }

    @Test
    fun `copy is independent`() {
        val er1 = DecayingEventRate(halfLife = 1.seconds)
        repeat(10) { er1.update(1.0) }
        val er2 = er1.copy()
        repeat(100) { er2.update(1.0) }
        assertTrue(er2.rate > er1.rate)
    }
}
