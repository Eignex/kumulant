package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.assertModesAgree
import com.eignex.kumulant.core.Concurrency
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class BucketHistogramStatTest {

    private fun BucketHistogramStat.rows(): List<Triple<Double, Double, Double>> = read().let { r ->
        r.weights.indices.map { Triple(r.lowerBounds[it], r.upperBounds[it], r.weights[it]) }
    }

    @Test
    fun `empty histogram has no rows`() {
        assertEquals(0, BucketHistogramStat(doubleArrayOf(0.0, 1.0)).read().weights.size)
    }

    @Test
    fun `each bucket includes its upper edge`() {
        val s = BucketHistogramStat(doubleArrayOf(0.0, 10.0, 100.0))
        listOf(-5.0, 0.0, 5.0, 10.0, 50.0, 100.0, 1000.0).forEach { s.update(it) }
        assertEquals(
            listOf(
                Triple(Double.NEGATIVE_INFINITY, 0.0, 2.0),
                Triple(0.0, 10.0, 2.0),
                Triple(10.0, 100.0, 2.0),
                Triple(100.0, Double.POSITIVE_INFINITY, 1.0),
            ),
            s.rows(),
        )
    }

    @Test
    fun `linear spreads equal-width buckets between the bounds`() {
        val s = BucketHistogramStat.linear(0.0, 100.0, 10)
        for (i in 1..100) s.update(i.toDouble())
        val r = s.read()
        assertEquals(10, r.weights.size)
        for (w in r.weights) assertEquals(10.0, w, 1e-9)
    }

    @Test
    fun `weights accumulate in the resolved bucket`() {
        val s = BucketHistogramStat(doubleArrayOf(0.0))
        s.update(value = 1.0, weight = 2.5)
        s.update(value = 1.0, weight = 1.5)
        assertEquals(listOf(Triple(0.0, Double.POSITIVE_INFINITY, 4.0)), s.rows())
    }

    @Test
    fun `merging a snapshot over the same edges reproduces it`() {
        val source = BucketHistogramStat.linear(0.0, 1.0, 100)
        for (v in listOf(-0.5, 0.295, 0.585, 0.135, 0.905, 1.0, 3.0)) source.update(v)
        val target = BucketHistogramStat.linear(0.0, 1.0, 100)
        target.merge(source.read())
        assertEquals(source.read(), target.read())
    }

    @Test
    fun `merging another layout rebins by interval midpoint`() {
        val target = BucketHistogramStat(doubleArrayOf(0.0, 10.0))
        target.merge(SparseHistogramResult(doubleArrayOf(2.0, 8.0), doubleArrayOf(4.0, 14.0), doubleArrayOf(3.0, 5.0)))
        assertEquals(
            listOf(Triple(0.0, 10.0, 3.0), Triple(10.0, Double.POSITIVE_INFINITY, 5.0)),
            target.rows(),
        )
    }

    @Test
    fun `invalid edges are rejected`() {
        assertFailsWith<IllegalArgumentException> { BucketHistogramStat(doubleArrayOf()) }
        assertFailsWith<IllegalArgumentException> { BucketHistogramStat(doubleArrayOf(1.0, 1.0)) }
        assertFailsWith<IllegalArgumentException> { BucketHistogramStat(doubleArrayOf(5.0, 3.0)) }
        assertFailsWith<IllegalArgumentException> { BucketHistogramStat(doubleArrayOf(0.0, Double.NaN)) }
        assertFailsWith<IllegalArgumentException> { BucketHistogramStat.linear(10.0, 0.0, 5) }
        assertFailsWith<IllegalArgumentException> { BucketHistogramStat.linear(0.0, 10.0, 0) }
    }

    @Test
    fun `sequential math equal across concurrency modes`() {
        val values = doubleArrayOf(1.0, -2.0, 3.5, 0.0, 4.2, -1.1, 7.0, 2.5)
        val reads = Concurrency.entries.associateWith { mode ->
            val s = BucketHistogramStat(doubleArrayOf(-1.0, 0.0, 1.0, 3.0), concurrency = mode)
            for (v in values) s.update(v)
            s.read()
        }
        assertModesAgree("BucketHistogramStat", reads)
    }
}
