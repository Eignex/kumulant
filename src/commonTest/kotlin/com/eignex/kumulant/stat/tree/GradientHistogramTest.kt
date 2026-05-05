package com.eignex.kumulant.stat.tree

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

private const val DELTA = 1e-12

class GradientHistogramTest {

    @Test
    fun `accumulates per-bin sums`() {
        val h = GradientHistogram(numBins = 4).apply {
            update(binIndex = 0, gradient = 1.0, hessian = 2.0)
            update(binIndex = 2, gradient = -3.0, hessian = 4.0)
            update(binIndex = 2, gradient = 5.0, hessian = 6.0, weight = 2.0)
        }
        val r = h.read(0L)
        assertEquals(4, r.numBins)
        assertContentEquals(doubleArrayOf(1.0, 0.0, -3.0 + 5.0 * 2.0, 0.0), r.sumG)
        assertContentEquals(doubleArrayOf(2.0, 0.0, 4.0 + 6.0 * 2.0, 0.0), r.sumH)
        assertContentEquals(doubleArrayOf(1.0, 0.0, 1.0 + 2.0, 0.0), r.totalWeights)
    }

    @Test
    fun `out of range bin throws`() {
        val h = GradientHistogram(3)
        assertFailsWith<IllegalArgumentException> { h.update(-1, 0.0, 0.0) }
        assertFailsWith<IllegalArgumentException> { h.update(3, 0.0, 0.0) }
    }

    @Test
    fun `zero weight is a noop`() {
        val h = GradientHistogram(2).apply { update(0, 99.0, 99.0, 0.0) }
        val r = h.read(0L)
        for (i in 0 until 2) {
            assertEquals(0.0, r.sumG[i], DELTA)
            assertEquals(0.0, r.sumH[i], DELTA)
            assertEquals(0.0, r.totalWeights[i], DELTA)
        }
    }

    @Test
    fun `merge two halves equals one accumulator`() {
        val a = GradientHistogram(3).apply {
            update(0, 1.0, 1.0)
            update(1, 2.0, 1.0)
        }
        val b = GradientHistogram(3).apply {
            update(1, 3.0, 1.0)
            update(2, 4.0, 1.0)
        }
        val ref = GradientHistogram(3).apply {
            update(0, 1.0, 1.0)
            update(1, 2.0, 1.0)
            update(1, 3.0, 1.0)
            update(2, 4.0, 1.0)
        }
        a.merge(b.read(0L))
        val merged = a.read(0L)
        val expected = ref.read(0L)
        assertContentEquals(expected.sumG, merged.sumG)
        assertContentEquals(expected.sumH, merged.sumH)
        assertContentEquals(expected.totalWeights, merged.totalWeights)
    }

    @Test
    fun `numBins mismatch on merge throws`() {
        val a = GradientHistogram(2)
        val b = GradientHistogram(3).apply { update(0, 1.0, 1.0) }
        assertFailsWith<IllegalArgumentException> { a.merge(b.read(0L)) }
    }

    @Test
    fun `reset zeros all bins`() {
        val h = GradientHistogram(2).apply {
            update(0, 1.0, 1.0)
            update(1, 2.0, 2.0)
            reset()
        }
        val r = h.read(0L)
        assertEquals(0.0, r.sumG[0], DELTA)
        assertEquals(0.0, r.sumG[1], DELTA)
        assertEquals(0.0, r.totalWeights.sum(), DELTA)
    }
}
