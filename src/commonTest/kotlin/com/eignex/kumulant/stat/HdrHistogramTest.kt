package com.eignex.kumulant.stat

import com.eignex.kumulant.core.SparseHistogramResult
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

private const val DELTA = 1e-9

class HdrHistogramTest {

    @Test
    fun `empty histogram has no populated buckets`() {
        val h = HdrHistogram()
        val r = h.read()
        assertEquals(0, r.lowerBounds.size)
        assertEquals(0, r.upperBounds.size)
        assertEquals(0, r.weights.size)
    }

    @Test
    fun `single value produces a single bucket whose range covers the input`() {
        val h = HdrHistogram(lowestDiscernibleValue = 0.001, initialHighestTrackableValue = 100.0)
        h.update(10.0)
        val r = h.read()
        assertEquals(1, r.weights.size)
        assertEquals(1.0, r.weights[0], DELTA)
        assertTrue(10.0 in r.lowerBounds[0]..r.upperBounds[0], "expected 10.0 in [${r.lowerBounds[0]}, ${r.upperBounds[0]}]")
    }

    @Test
    fun `multiple values at the same fine bucket accumulate in one bucket`() {
        val h = HdrHistogram(
            lowestDiscernibleValue = 0.001,
            initialHighestTrackableValue = 100.0,
            significantDigits = 2
        )
        repeat(10) { h.update(10.0) }
        val r = h.read()
        assertEquals(1, r.weights.size)
        assertEquals(10.0, r.weights[0], DELTA)
    }

    @Test
    fun `weights accumulate correctly`() {
        val h = HdrHistogram()
        h.update(5.0, weight = 2.0)
        h.update(5.0, weight = 3.0)
        val r = h.read()
        assertEquals(5.0, r.weights.sum(), DELTA)
    }

    @Test
    fun `negative values are silently ignored`() {
        val h = HdrHistogram()
        h.update(-1.0)
        h.update(5.0)
        val r = h.read()
        assertEquals(1.0, r.weights.sum(), DELTA)
    }

    @Test
    fun `zero weight update is ignored`() {
        val h = HdrHistogram()
        h.update(5.0, weight = 0.0)
        val r = h.read()
        assertEquals(0, r.weights.size)
    }

    @Test
    fun `histogram auto-resizes to accept values beyond initialHighestTrackableValue`() {
        val h = HdrHistogram(
            lowestDiscernibleValue = 0.001,
            initialHighestTrackableValue = 10.0
        )
        h.update(1.0)
        h.update(5000.0) // way past initial highest
        val r = h.read()
        assertEquals(2.0, r.weights.sum(), DELTA)
        val totalRange = r.upperBounds.maxOrNull()!!
        assertTrue(totalRange >= 5000.0, "expected resize to cover 5000, got upper=$totalRange")
    }

    @Test
    fun `reset clears counts`() {
        val h = HdrHistogram()
        repeat(100) { h.update(5.0) }
        h.reset()
        assertEquals(0, h.read().weights.size)
    }

    @Test
    fun `merge re-adds each bucket from incoming result`() {
        val h1 = HdrHistogram()
        h1.update(1.0)
        h1.update(10.0)

        val h2 = HdrHistogram()
        h2.update(1.0)
        h2.update(100.0)

        h1.merge(h2.read())
        val merged = h1.read()
        assertEquals(4.0, merged.weights.sum(), DELTA)
    }

    @Test
    fun `merge with empty source is a no-op`() {
        val h1 = HdrHistogram()
        h1.update(5.0)
        val before = h1.read().weights.sum()

        h1.merge(SparseHistogramResult(DoubleArray(0), DoubleArray(0), DoubleArray(0)))
        assertEquals(before, h1.read().weights.sum(), DELTA)
    }

    @Test
    fun `create returns an independent histogram`() {
        val h1 = HdrHistogram()
        h1.update(5.0)
        val h2 = h1.create()
        repeat(10) { h2.update(5.0) }
        assertEquals(1.0, h1.read().weights.sum(), DELTA)
        assertEquals(10.0, h2.read().weights.sum(), DELTA)
    }

    @Test
    fun `buckets are sorted by lower bound`() {
        val h = HdrHistogram()
        for (v in listOf(50.0, 1.0, 10.0, 100.0, 5.0)) h.update(v)
        val r = h.read()
        for (i in 1 until r.lowerBounds.size) {
            assertTrue(
                r.lowerBounds[i] >= r.lowerBounds[i - 1],
                "buckets not sorted: ${r.lowerBounds.toList()}"
            )
        }
    }

    @Test
    fun `each bucket's lower bound does not exceed its upper bound`() {
        val h = HdrHistogram()
        for (v in listOf(0.01, 1.0, 10.0, 100.0, 1000.0)) h.update(v)
        val r = h.read()
        for (i in r.lowerBounds.indices) {
            assertTrue(r.lowerBounds[i] <= r.upperBounds[i])
        }
    }

    @Test
    fun `invalid lowestDiscernibleValue throws`() {
        assertFailsWith<IllegalArgumentException> {
            HdrHistogram(lowestDiscernibleValue = 0.0)
        }
        assertFailsWith<IllegalArgumentException> {
            HdrHistogram(lowestDiscernibleValue = -0.1)
        }
    }

    @Test
    fun `invalid initialHighestTrackableValue throws`() {
        assertFailsWith<IllegalArgumentException> {
            HdrHistogram(
                lowestDiscernibleValue = 1.0,
                initialHighestTrackableValue = 1.5
            )
        }
    }

    @Test
    fun `invalid significantDigits throws`() {
        assertFailsWith<IllegalArgumentException> { HdrHistogram(significantDigits = 0) }
        assertFailsWith<IllegalArgumentException> { HdrHistogram(significantDigits = 6) }
    }

    @Test
    fun `value zero is trackable`() {
        val h = HdrHistogram()
        h.update(0.0)
        val r = h.read()
        assertEquals(1.0, r.weights.sum(), DELTA)
    }
}
