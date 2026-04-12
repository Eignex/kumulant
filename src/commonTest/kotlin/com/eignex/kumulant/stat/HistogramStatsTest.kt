package com.eignex.kumulant.stat

import com.eignex.kumulant.core.QuantileResult
import com.eignex.kumulant.core.toSparseHistogram
import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class FrugalQuantileTest {

    @Test
    fun `estimate moves up when value is above it`() {
        val fq = FrugalQuantile(q = 0.5, stepSize = 1.0, initialEstimate = 0.0)
        fq.update(100.0)
        assertTrue(fq.quantile > 0.0)
    }

    @Test
    fun `estimate moves down when value is below it`() {
        val fq = FrugalQuantile(q = 0.5, stepSize = 1.0, initialEstimate = 100.0)
        fq.update(0.0)
        assertTrue(fq.quantile < 100.0)
    }

    @Test
    fun `estimate does not move when value equals it`() {
        val fq = FrugalQuantile(q = 0.5, stepSize = 1.0, initialEstimate = 5.0)
        fq.update(5.0)
        assertEquals(5.0, fq.quantile)
    }

    @Test
    fun `q=1 estimate only moves up`() {
        val fq = FrugalQuantile(q = 1.0, stepSize = 1.0, initialEstimate = 0.0)
        fq.update(50.0)
        val after = fq.quantile
        fq.update(0.0) // below current estimate; for q=1 delta=-stepSize*(1-1)=0
        assertEquals(after, fq.quantile)
    }

    @Test
    fun `q=0 estimate only moves down`() {
        val fq = FrugalQuantile(q = 0.0, stepSize = 1.0, initialEstimate = 100.0)
        fq.update(0.0)
        val after = fq.quantile
        fq.update(200.0) // above current estimate; for q=0 delta=stepSize*0=0
        assertEquals(after, fq.quantile)
    }

    @Test
    fun `estimate stays near median of symmetric alternating stream`() {
        // Starting near the true median (50), estimate should remain close
        val fq = FrugalQuantile(q = 0.5, stepSize = 1.0, initialEstimate = 50.0)
        repeat(2000) {
            fq.update(0.0)
            fq.update(100.0)
        }
        // True median is 50; starting at 50, estimate oscillates in a tight band
        assertTrue(fq.quantile in 40.0..60.0, "quantile=${fq.quantile}")
    }

    @Test
    fun `zero weight update is ignored`() {
        val fq = FrugalQuantile(q = 0.5, stepSize = 1.0, initialEstimate = 5.0)
        fq.update(100.0, weight = 0.0)
        assertEquals(5.0, fq.quantile)
    }

    @Test
    fun `reset returns to initial estimate`() {
        val fq = FrugalQuantile(q = 0.5, stepSize = 1.0, initialEstimate = 3.0)
        repeat(100) { fq.update(100.0) }
        fq.reset()
        assertEquals(3.0, fq.quantile)
    }

    @Test
    fun `merge averages two estimates`() {
        val fq = FrugalQuantile(q = 0.5, stepSize = 1.0, initialEstimate = 20.0)
        fq.merge(QuantileResult(0.5, 40.0))
        assertEquals(30.0, fq.quantile)
    }

    @Test
    fun `create produces fresh independent stat`() {
        val fq1 = FrugalQuantile(q = 0.5, stepSize = 1.0, initialEstimate = 0.0)
        val fq2 = fq1.create()
        repeat(100) { fq2.update(100.0) }
        assertEquals(0.0, fq1.quantile) // fq1 unchanged
        assertTrue(fq2.quantile > fq1.quantile)
    }

    @Test
    fun `invalid q throws`() {
        assertFailsWith<IllegalArgumentException> { FrugalQuantile(q = -0.1) }
        assertFailsWith<IllegalArgumentException> { FrugalQuantile(q = 1.1) }
    }
}

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
        // All quantiles should be near 42 (within relative error)
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
        // p50 should be at or near 0
        assertTrue(r.quantiles[0] <= 1.0, "p50=${r.quantiles[0]}")
    }

    @Test
    fun `handles negative values`() {
        val sketch = DDSketch(probabilities = doubleArrayOf(0.5))
        for (i in -50..-1) sketch.update(i.toDouble())
        for (i in 1..50) sketch.update(i.toDouble())
        val q50 = sketch.read().quantiles[0]
        // True median is between -1 and 1; sketch should be in that zone
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
        // s1 should still reflect only 10 observations
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
        // total weight in histogram equals total in sketch
        assertEquals(10.0, hist.weights.sum(), 1e-9)
    }
}
