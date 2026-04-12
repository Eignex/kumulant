package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.AtomicMode
import com.eignex.kumulant.concurrent.SerialMode
import com.eignex.kumulant.core.CountResult
import kotlin.test.Test
import kotlin.test.assertEquals

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
    fun `create produces fresh independent stat`() {
        val c1 = Count(AtomicMode).apply { update(1.0) }
        val c2 = c1.create(SerialMode)
        c2.update(2.0)
        assertEquals(1L, c1.count)
        assertEquals(1L, c2.count)
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
        val tw = TotalWeights()
        tw.update(0.0, weight = 3.5)
        val r = tw.read()
        assertEquals(3.5, r.sum, DELTA)
    }
}
