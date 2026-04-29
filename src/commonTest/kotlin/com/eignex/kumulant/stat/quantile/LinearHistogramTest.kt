package com.eignex.kumulant.stat.quantile

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class LinearHistogramTest {

    @Test
    fun `empty histogram has no rows`() {
        val h = LinearHistogram(0.0, 10.0, 10)
        val r = h.read()
        assertEquals(0, r.weights.size)
    }

    @Test
    fun `single value lands in correct bin`() {
        val h = LinearHistogram(0.0, 10.0, 10)
        h.update(3.5)
        val r = h.read()
        assertEquals(1, r.weights.size)
        assertEquals(3.0, r.lowerBounds[0], 1e-9)
        assertEquals(4.0, r.upperBounds[0], 1e-9)
        assertEquals(1.0, r.weights[0], 1e-9)
    }

    @Test
    fun `uniform spread across bins`() {
        val h = LinearHistogram(0.0, 100.0, 10)
        for (i in 0..99) h.update(i.toDouble())
        val r = h.read()
        assertEquals(10, r.weights.size)
        for (w in r.weights) assertEquals(10.0, w, 1e-9)
    }

    @Test
    fun `value below lower goes to underflow`() {
        val h = LinearHistogram(0.0, 10.0, 10)
        h.update(-1.0, weight = 3.0)
        val r = h.read()
        assertEquals(1, r.weights.size)
        assertEquals(Double.NEGATIVE_INFINITY, r.lowerBounds[0])
        assertEquals(0.0, r.upperBounds[0], 1e-9)
        assertEquals(3.0, r.weights[0], 1e-9)
    }

    @Test
    fun `value at or above upper goes to overflow`() {
        val h = LinearHistogram(0.0, 10.0, 10)
        h.update(10.0)
        h.update(15.0, weight = 2.0)
        val r = h.read()
        assertEquals(1, r.weights.size)
        assertEquals(10.0, r.lowerBounds[0], 1e-9)
        assertEquals(Double.POSITIVE_INFINITY, r.upperBounds[0])
        assertEquals(3.0, r.weights[0], 1e-9)
    }

    @Test
    fun `weighted updates accumulate`() {
        val h = LinearHistogram(0.0, 10.0, 10)
        h.update(5.0, weight = 4.0)
        h.update(5.0, weight = 6.0)
        val r = h.read()
        assertEquals(1, r.weights.size)
        assertEquals(10.0, r.weights[0], 1e-9)
    }

    @Test
    fun `zero weight ignored`() {
        val h = LinearHistogram(0.0, 10.0, 10)
        h.update(5.0, weight = 0.0)
        h.update(5.0, weight = -1.0)
        assertEquals(0, h.read().weights.size)
    }

    @Test
    fun `merge combines distributions`() {
        val a = LinearHistogram(0.0, 100.0, 10)
        val b = LinearHistogram(0.0, 100.0, 10)
        for (i in 0..49) a.update(i.toDouble())
        for (i in 50..99) b.update(i.toDouble())
        a.merge(b.read())
        assertEquals(100.0, a.read().weights.sum(), 1e-9)
    }

    @Test
    fun `reset clears state`() {
        val h = LinearHistogram(0.0, 10.0, 10)
        for (i in 0..9) h.update(i.toDouble())
        h.reset()
        assertEquals(0, h.read().weights.size)
    }

    @Test
    fun `create produces independent stat`() {
        val a = LinearHistogram(0.0, 10.0, 10)
        a.update(1.0)
        val b = a.create()
        b.update(2.0)
        b.update(2.0)
        assertEquals(1.0, a.read().weights.sum(), 1e-9)
        assertEquals(2.0, b.read().weights.sum(), 1e-9)
    }

    @Test
    fun `invalid args throw`() {
        assertFailsWith<IllegalArgumentException> { LinearHistogram(10.0, 0.0, 5) }
        assertFailsWith<IllegalArgumentException> { LinearHistogram(0.0, 10.0, 0) }
        assertFailsWith<IllegalArgumentException> { LinearHistogram(0.0, 10.0, -1) }
    }
}
