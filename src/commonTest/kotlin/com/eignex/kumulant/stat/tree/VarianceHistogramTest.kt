package com.eignex.kumulant.stat.tree

import com.eignex.kumulant.stat.summary.Variance
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

private const val DELTA = 1e-9

class VarianceHistogramTest {

    @Test
    fun `each bin matches an independent Variance`() {
        val rows = listOf(
            0 to 1.0,
            0 to 2.0,
            0 to 3.0,
            1 to 10.0,
            1 to 12.0,
            2 to 100.0,
        )
        val h = VarianceHistogram(numBins = 3)
        val refs = Array(3) { Variance() }
        for ((bin, value) in rows) {
            h.update(bin, value)
            refs[bin].update(value)
        }
        val r = h.read(0L)
        for (i in 0 until 3) {
            val expected = refs[i].read(0L)
            assertEquals(expected.totalWeights, r.totalWeights[i], DELTA)
            assertEquals(expected.mean, r.mean[i], DELTA)
            assertEquals(expected.variance, r.variance[i], DELTA)
        }
    }

    @Test
    fun `out of range bin throws`() {
        val h = VarianceHistogram(2)
        assertFailsWith<IllegalArgumentException> { h.update(-1, 1.0) }
        assertFailsWith<IllegalArgumentException> { h.update(2, 1.0) }
    }

    @Test
    fun `merge two halves equals one accumulator`() {
        val a = VarianceHistogram(2).apply {
            update(0, 1.0); update(0, 2.0); update(1, 5.0)
        }
        val b = VarianceHistogram(2).apply {
            update(0, 3.0); update(1, 7.0); update(1, 9.0)
        }
        val ref = VarianceHistogram(2).apply {
            update(0, 1.0); update(0, 2.0); update(0, 3.0)
            update(1, 5.0); update(1, 7.0); update(1, 9.0)
        }
        a.merge(b.read(0L))
        val merged = a.read(0L)
        val expected = ref.read(0L)
        for (i in 0 until 2) {
            assertEquals(expected.totalWeights[i], merged.totalWeights[i], DELTA)
            assertEquals(expected.mean[i], merged.mean[i], DELTA)
            assertEquals(expected.variance[i], merged.variance[i], DELTA)
        }
    }

    @Test
    fun `reset clears all bins`() {
        val h = VarianceHistogram(2).apply {
            update(0, 1.0); update(1, 2.0)
            reset()
        }
        val r = h.read(0L)
        for (i in 0 until 2) {
            assertEquals(0.0, r.totalWeights[i], DELTA)
            assertEquals(0.0, r.mean[i], DELTA)
            assertEquals(0.0, r.variance[i], DELTA)
        }
    }
}
