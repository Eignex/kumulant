package com.eignex.kumulant.stat.tree

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

private const val DELTA = 1e-12

class ClassHistogramTest {

    @Test
    fun `accumulates per bin per class`() {
        val h = ClassHistogram(numBins = 3, numClasses = 2).apply {
            update(binIndex = 0, classIndex = 0)
            update(binIndex = 0, classIndex = 1, weight = 2.0)
            update(binIndex = 2, classIndex = 1)
        }
        val r = h.read(0L)
        assertEquals(1.0, r.count(0, 0), DELTA)
        assertEquals(2.0, r.count(0, 1), DELTA)
        assertEquals(0.0, r.count(1, 0), DELTA)
        assertEquals(0.0, r.count(1, 1), DELTA)
        assertEquals(0.0, r.count(2, 0), DELTA)
        assertEquals(1.0, r.count(2, 1), DELTA)
    }

    @Test
    fun `out of range indices throw`() {
        val h = ClassHistogram(2, 2)
        assertFailsWith<IllegalArgumentException> { h.update(-1, 0) }
        assertFailsWith<IllegalArgumentException> { h.update(2, 0) }
        assertFailsWith<IllegalArgumentException> { h.update(0, -1) }
        assertFailsWith<IllegalArgumentException> { h.update(0, 2) }
    }

    @Test
    fun `row major layout`() {
        // Layout: counts[bin * numClasses + cls]
        val h = ClassHistogram(3, 4).apply {
            update(2, 3, weight = 7.0)
        }
        val r = h.read(0L)
        assertEquals(7.0, r.counts[2 * 4 + 3], DELTA)
        // All other slots zero.
        for (i in r.counts.indices) {
            if (i != 2 * 4 + 3) assertEquals(0.0, r.counts[i], DELTA)
        }
    }

    @Test
    fun `merge adds component wise`() {
        val a = ClassHistogram(2, 2).apply {
            update(0, 0)
            update(1, 1)
        }
        val b = ClassHistogram(2, 2).apply {
            update(0, 0, weight = 3.0)
            update(0, 1, weight = 2.0)
        }
        a.merge(b.read(0L))
        val r = a.read(0L)
        assertEquals(4.0, r.count(0, 0), DELTA)
        assertEquals(2.0, r.count(0, 1), DELTA)
        assertEquals(0.0, r.count(1, 0), DELTA)
        assertEquals(1.0, r.count(1, 1), DELTA)
    }

    @Test
    fun `shape mismatch on merge throws`() {
        val a = ClassHistogram(2, 2)
        val b = ClassHistogram(3, 2).apply { update(0, 0) }
        assertFailsWith<IllegalArgumentException> { a.merge(b.read(0L)) }
        val c = ClassHistogram(2, 3).apply { update(0, 0) }
        assertFailsWith<IllegalArgumentException> { a.merge(c.read(0L)) }
    }
}
