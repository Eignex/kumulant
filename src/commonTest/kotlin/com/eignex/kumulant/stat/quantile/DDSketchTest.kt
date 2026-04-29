package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.stat.quantile.SketchResult

import com.eignex.kumulant.stat.quantile.toSparseHistogram

import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class DDSketchTest {

    @Test
    fun `empty sketch returns zero quantiles`() {
        val sketch = DDSketch(probabilities = doubleArrayOf(0.5, 0.9))
        val r = sketch.read()
        assertEquals(0.0, r.quantiles[0])
        assertEquals(0.0, r.quantiles[1])
        assertEquals(0.0, r.totalWeights)
    }

    @Test
    fun `single value returns that value as all quantiles`() {
        val sketch = DDSketch(relativeError = 0.01, probabilities = doubleArrayOf(0.1, 0.5, 0.9))
        sketch.update(42.0)
        val r = sketch.read()

        for (q in r.quantiles) {
            assertTrue(abs(q - 42.0) <= 42.0 * 0.02 + 1.0, "quantile=$q expected near 42")
        }
    }

    @Test
    fun `p50 of 1 to 100 is near 50`() {
        val sketch = DDSketch(relativeError = 0.01, probabilities = doubleArrayOf(0.5))
        for (i in 1..100) sketch.update(i.toDouble())
        val q50 = sketch.read().quantiles[0]
        assertTrue(q50 in 45.0..56.0, "p50=$q50")
    }

    @Test
    fun `p90 of 1 to 100 is near 90`() {
        val sketch = DDSketch(relativeError = 0.01, probabilities = doubleArrayOf(0.9))
        for (i in 1..100) sketch.update(i.toDouble())
        val q90 = sketch.read().quantiles[0]
        assertTrue(q90 in 85.0..96.0, "p90=$q90")
    }

    @Test
    fun `quantiles are monotonically non-decreasing`() {
        val sketch = DDSketch(
            relativeError = 0.01,
            probabilities = doubleArrayOf(0.25, 0.5, 0.75, 0.9, 0.99)
        )
        for (i in 1..1000) sketch.update(i.toDouble())
        val qs = sketch.read().quantiles
        for (i in 0 until qs.size - 1) {
            assertTrue(qs[i] <= qs[i + 1], "quantiles not sorted: ${qs.toList()}")
        }
    }

    @Test
    fun `handles zero values`() {
        val sketch = DDSketch(probabilities = doubleArrayOf(0.5))
        repeat(50) { sketch.update(0.0) }
        repeat(50) { sketch.update(10.0) }
        val r = sketch.read()
        assertEquals(100.0, r.totalWeights)
        assertTrue(r.zeroCount > 0.0)

        assertTrue(r.quantiles[0] <= 1.0, "p50=${r.quantiles[0]}")
    }

    @Test
    fun `handles negative values`() {
        val sketch = DDSketch(probabilities = doubleArrayOf(0.5))
        for (i in -50..-1) sketch.update(i.toDouble())
        for (i in 1..50) sketch.update(i.toDouble())
        val q50 = sketch.read().quantiles[0]

        assertTrue(q50 in -5.0..5.0, "p50=$q50")
    }

    @Test
    fun `total weights accumulate correctly`() {
        val sketch = DDSketch()
        repeat(10) { sketch.update(1.0) }
        repeat(10) { sketch.update(1.0, weight = 2.0) }
        assertEquals(30.0, sketch.read().totalWeights)
    }

    @Test
    fun `zero weight update is ignored`() {
        val sketch = DDSketch(probabilities = doubleArrayOf(0.5))
        sketch.update(100.0, weight = 0.0)
        assertEquals(0.0, sketch.read().totalWeights)
    }

    @Test
    fun `merge combines two distributions`() {
        val s1 = DDSketch(relativeError = 0.01, probabilities = doubleArrayOf(0.5))
        val s2 = DDSketch(relativeError = 0.01, probabilities = doubleArrayOf(0.5))
        for (i in 1..50) s1.update(i.toDouble())
        for (i in 51..100) s2.update(i.toDouble())
        s1.merge(s2.read())
        val q50 = s1.read().quantiles[0]
        assertTrue(q50 in 45.0..56.0, "merged p50=$q50")
        assertEquals(100.0, s1.read().totalWeights)
    }

    @Test
    fun `reset clears all state`() {
        val sketch = DDSketch(probabilities = doubleArrayOf(0.5))
        for (i in 1..100) sketch.update(i.toDouble())
        sketch.reset()
        val r = sketch.read()
        assertEquals(0.0, r.totalWeights)
        assertEquals(0.0, r.quantiles[0])
    }

    @Test
    fun `create produces fresh independent stat`() {
        val s1 = DDSketch(relativeError = 0.01, probabilities = doubleArrayOf(0.5))
        for (i in 1..10) s1.update(i.toDouble())
        val s2 = s1.create()
        for (i in 1..1000) s2.update(i.toDouble())

        assertEquals(10.0, s1.read().totalWeights)
    }

    @Test
    fun `invalid relative error throws`() {
        assertFailsWith<IllegalArgumentException> { DDSketch(relativeError = -0.01) }
        assertFailsWith<IllegalArgumentException> { DDSketch(relativeError = 1.1) }
    }

    @Test
    fun `toSparseHistogram has matching bounds and weights`() {
        val sketch = DDSketch(relativeError = 0.01, probabilities = doubleArrayOf(0.5))
        for (i in 1..10) sketch.update(i.toDouble())
        val hist = sketch.read().toSparseHistogram()
        assertEquals(hist.lowerBounds.size, hist.upperBounds.size)
        assertEquals(hist.lowerBounds.size, hist.weights.size)
        assertTrue(hist.weights.all { it > 0.0 })

        assertEquals(10.0, hist.weights.sum(), 1e-9)
    }
}

class DDSketchEdgeCasesTest {

    @Test
    fun `merge with incompatible relative error throws`() {
        val a = DDSketch(relativeError = 0.01, probabilities = doubleArrayOf(0.5))
        a.update(10.0)

        val b = DDSketch(relativeError = 0.05, probabilities = doubleArrayOf(0.5))
        b.update(20.0)

        assertFailsWith<IllegalArgumentException> {
            a.merge(b.read())
        }
    }

    @Test
    fun `merge with identical relative error succeeds`() {
        val a = DDSketch(relativeError = 0.01, probabilities = doubleArrayOf(0.5))
        a.update(10.0)

        val b = DDSketch(relativeError = 0.01, probabilities = doubleArrayOf(0.5))
        b.update(20.0)

        a.merge(b.read())
        assertEquals(2.0, a.read().totalWeights, 1e-9)
    }

    @Test
    fun `merge with empty sketch is a no-op on existing data`() {
        val a = DDSketch(relativeError = 0.01, probabilities = doubleArrayOf(0.5))
        repeat(10) { a.update(5.0) }
        val before = a.read()

        a.merge(
            SketchResult(
                probabilities = before.probabilities,
                quantiles = DoubleArray(before.probabilities.size),
                gamma = before.gamma,
                totalWeights = 0.0,
                zeroCount = 0.0,
                positiveBins = emptyMap(),
                negativeBins = emptyMap(),
            )
        )

        assertEquals(10.0, a.read().totalWeights, 1e-9)
    }
}
