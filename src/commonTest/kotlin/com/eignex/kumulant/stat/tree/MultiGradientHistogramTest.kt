package com.eignex.kumulant.stat.tree

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

private const val DELTA = 1e-12

class MultiGradientHistogramTest {

    @Test
    fun `each feature matches an independent GradientHistogram`() {
        val numFeatures = 3
        val numBins = 4
        val rows = listOf(
            Triple(intArrayOf(0, 1, 2), 1.0, 0.5),
            Triple(intArrayOf(2, 1, 0), -2.0, 1.5),
            Triple(intArrayOf(3, 3, 3), 0.5, 0.25),
        )
        val multi = MultiGradientHistogram(numFeatures, numBins)
        val refs = Array(numFeatures) { GradientHistogram(numBins) }
        for ((bins, g, h) in rows) {
            multi.update(bins, g, h)
            for (f in 0 until numFeatures) refs[f].update(bins[f], g, h)
        }
        val mr = multi.read(0L)
        for (f in 0 until numFeatures) {
            val expected = refs[f].read(0L)
            for (b in 0 until numBins) {
                assertEquals(expected.sumG[b], mr.sumG(f, b), DELTA)
                assertEquals(expected.sumH[b], mr.sumH(f, b), DELTA)
                assertEquals(expected.totalWeights[b], mr.totalWeight(f, b), DELTA)
            }
        }
    }

    @Test
    fun `forFeature slice equals an independent reference`() {
        val numFeatures = 2
        val numBins = 3
        val multi = MultiGradientHistogram(numFeatures, numBins).apply {
            update(intArrayOf(0, 2), 1.0, 0.5)
            update(intArrayOf(1, 1), 2.0, 0.25)
        }
        val refF0 = GradientHistogram(numBins).apply {
            update(0, 1.0, 0.5)
            update(1, 2.0, 0.25)
        }
        val mrF0 = multi.read(0L).forFeature(0)
        val expF0 = refF0.read(0L)
        assertContentEquals(expF0.sumG, mrF0.sumG)
        assertContentEquals(expF0.sumH, mrF0.sumH)
        assertContentEquals(expF0.totalWeights, mrF0.totalWeights)
    }

    @Test
    fun `featureBins size mismatch throws`() {
        val multi = MultiGradientHistogram(numFeatures = 3, numBins = 4)
        assertFailsWith<IllegalArgumentException> {
            multi.update(intArrayOf(0, 1), 1.0, 1.0)
        }
    }

    @Test
    fun `out of range bin throws`() {
        val multi = MultiGradientHistogram(numFeatures = 2, numBins = 3)
        assertFailsWith<IllegalArgumentException> {
            multi.update(intArrayOf(0, 5), 1.0, 1.0)
        }
        assertFailsWith<IllegalArgumentException> {
            multi.update(intArrayOf(-1, 0), 1.0, 1.0)
        }
    }

    @Test
    fun `merge two halves equals one accumulator`() {
        val a = MultiGradientHistogram(2, 3).apply {
            update(intArrayOf(0, 1), 1.0, 0.5)
            update(intArrayOf(1, 2), 2.0, 1.0)
        }
        val b = MultiGradientHistogram(2, 3).apply {
            update(intArrayOf(2, 0), 3.0, 0.25)
            update(intArrayOf(0, 0), 4.0, 1.5)
        }
        val ref = MultiGradientHistogram(2, 3).apply {
            update(intArrayOf(0, 1), 1.0, 0.5)
            update(intArrayOf(1, 2), 2.0, 1.0)
            update(intArrayOf(2, 0), 3.0, 0.25)
            update(intArrayOf(0, 0), 4.0, 1.5)
        }
        a.merge(b.read(0L))
        val mr = a.read(0L)
        val rr = ref.read(0L)
        assertContentEquals(rr.sumG, mr.sumG)
        assertContentEquals(rr.sumH, mr.sumH)
        assertContentEquals(rr.totalWeights, mr.totalWeights)
    }

    @Test
    fun `shape mismatch on merge throws`() {
        val a = MultiGradientHistogram(2, 3)
        val b = MultiGradientHistogram(3, 3).apply { update(intArrayOf(0, 0, 0), 1.0, 1.0) }
        assertFailsWith<IllegalArgumentException> { a.merge(b.read(0L)) }
        val c = MultiGradientHistogram(2, 4).apply { update(intArrayOf(0, 0), 1.0, 1.0) }
        assertFailsWith<IllegalArgumentException> { a.merge(c.read(0L)) }
    }

    @Test
    fun `zero weight is a noop`() {
        val multi = MultiGradientHistogram(2, 2).apply {
            update(intArrayOf(0, 1), 99.0, 99.0, weight = 0.0)
        }
        val r = multi.read(0L)
        for (i in r.totalWeights.indices) assertEquals(0.0, r.totalWeights[i], DELTA)
    }
}
