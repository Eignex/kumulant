package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.DELTA
import kotlin.math.abs
import kotlin.math.pow
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class DDSketchTest {
    @Test
    fun `quantiles follow weighted order across signed bin indices and zero`() {
        val sketch = DDSketchStat(
            relativeError = 1.0 / 3.0,
            probabilities = doubleArrayOf(0.0, 0.25, 8.0 / 28.0, 13.0 / 28.0, 24.0 / 28.0, 1.0),
        )
        val empty = sketch.read()
        val gamma = empty.gamma
        sketch.merge(
            empty.copy(
                totalWeights = 28.0,
                zeroCount = 11.0,
                positiveBins = mapOf(3 to 2.0, -2 to 3.0),
                negativeBins = mapOf(-1 to 5.0, 2 to 7.0),
            ),
        )
        val expected = doubleArrayOf(
            -2.0 * gamma.pow(2) / (1.0 + gamma),
            -2.0 * gamma.pow(2) / (1.0 + gamma),
            -2.0 * gamma.pow(-1) / (1.0 + gamma),
            0.0,
            2.0 * gamma.pow(-2) / (1.0 + gamma),
            2.0 * gamma.pow(3) / (1.0 + gamma),
        )

        val result = sketch.read()

        for (i in expected.indices) assertEquals(expected[i], result.quantiles[i], 1e-12)
    }

    @Test
    fun `histogram orders shuffled bins with their original weights`() {
        val result = SketchResult(
            probabilities = doubleArrayOf(),
            quantiles = doubleArrayOf(),
            gamma = 2.0,
            totalWeights = 28.0,
            zeroCount = 11.0,
            positiveBins = mapOf(3 to 2.0, -2 to 3.0),
            negativeBins = mapOf(-1 to 5.0, 2 to 7.0),
        )

        val histogram = result.toSparseHistogram()

        assertTrue(histogram.lowerBounds.contentEquals(doubleArrayOf(-4.0, -0.5, 0.0, 0.125, 4.0)))
        assertTrue(histogram.upperBounds.contentEquals(doubleArrayOf(-2.0, -0.25, 0.0, 0.25, 8.0)))
        assertTrue(histogram.weights.contentEquals(doubleArrayOf(7.0, 5.0, 11.0, 3.0, 2.0)))
    }

    @Test
    fun `empty sketch returns NaN quantiles`() {
        val sketch = DDSketchStat(probabilities = doubleArrayOf(0.5, 0.9))
        val r = sketch.read()
        assertTrue(r.quantiles[0].isNaN(), "p50 was ${r.quantiles[0]}")
        assertTrue(r.quantiles[1].isNaN(), "p90 was ${r.quantiles[1]}")
        assertEquals(0.0, r.totalWeights)
        assertTrue(r.isEmpty)
    }

    @Test
    fun `single value returns that value as all quantiles`() {
        val sketch = DDSketchStat(relativeError = 0.01, probabilities = doubleArrayOf(0.1, 0.5, 0.9))
        sketch.update(42.0)
        val r = sketch.read()

        for (q in r.quantiles) {
            assertTrue(abs(q - 42.0) <= 42.0 * 0.02 + 1.0, "quantile=$q expected near 42")
        }
    }

    @Test
    fun `p50 of 1 to 100 is near 50`() {
        val sketch = DDSketchStat(relativeError = 0.01, probabilities = doubleArrayOf(0.5))
        for (i in 1..100) sketch.update(i.toDouble())
        val q50 = sketch.read().quantiles[0]
        assertTrue(q50 in 45.0..56.0, "p50=$q50")
    }

    @Test
    fun `p90 of 1 to 100 is near 90`() {
        val sketch = DDSketchStat(relativeError = 0.01, probabilities = doubleArrayOf(0.9))
        for (i in 1..100) sketch.update(i.toDouble())
        val q90 = sketch.read().quantiles[0]
        assertTrue(q90 in 85.0..96.0, "p90=$q90")
    }

    @Test
    fun `quantiles are monotonically non-decreasing`() {
        val sketch = DDSketchStat(
            relativeError = 0.01,
            probabilities = doubleArrayOf(0.25, 0.5, 0.75, 0.9, 0.99),
        )
        for (i in 1..1000) sketch.update(i.toDouble())
        val qs = sketch.read().quantiles
        for (i in 0 until qs.size - 1) {
            assertTrue(qs[i] <= qs[i + 1], "quantiles not sorted: ${qs.toList()}")
        }
    }

    @Test
    fun `handles zero values`() {
        val sketch = DDSketchStat(probabilities = doubleArrayOf(0.5))
        repeat(50) { sketch.update(0.0) }
        repeat(50) { sketch.update(10.0) }
        val r = sketch.read()
        assertEquals(100.0, r.totalWeights)
        assertTrue(r.zeroCount > 0.0)

        assertTrue(r.quantiles[0] <= 1.0, "p50=${r.quantiles[0]}")
    }

    @Test
    fun `handles negative values`() {
        val sketch = DDSketchStat(probabilities = doubleArrayOf(0.5))
        for (i in -50..-1) sketch.update(i.toDouble())
        for (i in 1..50) sketch.update(i.toDouble())
        val q50 = sketch.read().quantiles[0]

        assertTrue(q50 in -5.0..5.0, "p50=$q50")
    }

    @Test
    fun `total weights accumulate correctly`() {
        val sketch = DDSketchStat()
        repeat(10) { sketch.update(1.0) }
        repeat(10) { sketch.update(1.0, weight = 2.0) }
        assertEquals(30.0, sketch.read().totalWeights)
    }

    @Test
    fun `merge combines two distributions`() {
        val s1 = DDSketchStat(relativeError = 0.01, probabilities = doubleArrayOf(0.5))
        val s2 = DDSketchStat(relativeError = 0.01, probabilities = doubleArrayOf(0.5))
        for (i in 1..50) s1.update(i.toDouble())
        for (i in 51..100) s2.update(i.toDouble())
        s1.merge(s2.read())
        val q50 = s1.read().quantiles[0]
        assertTrue(q50 in 45.0..56.0, "merged p50=$q50")
        assertEquals(100.0, s1.read().totalWeights)
    }

    @Test
    fun `invalid relative error throws`() {
        assertFailsWith<IllegalArgumentException> { DDSketchStat(relativeError = -0.01) }
        assertFailsWith<IllegalArgumentException> { DDSketchStat(relativeError = 1.1) }
        assertFailsWith<IllegalArgumentException> { DDSketchStat(relativeError = 0.0) }
        assertFailsWith<IllegalArgumentException> { DDSketchStat(relativeError = 1.0) }
    }

    @Test
    fun `toSparseHistogram has matching bounds and weights`() {
        val sketch = DDSketchStat(relativeError = 0.01, probabilities = doubleArrayOf(0.5))
        for (i in 1..10) sketch.update(i.toDouble())
        val hist = sketch.read().toSparseHistogram()
        assertEquals(hist.lowerBounds.size, hist.upperBounds.size)
        assertEquals(hist.lowerBounds.size, hist.weights.size)
        assertTrue(hist.weights.all { it > 0.0 })

        assertEquals(10.0, hist.weights.sum(), 1e-9)
    }

    @Test
    fun `merge with incompatible relative error throws`() {
        val a = DDSketchStat(relativeError = 0.01, probabilities = doubleArrayOf(0.5))
        a.update(10.0)

        val b = DDSketchStat(relativeError = 0.05, probabilities = doubleArrayOf(0.5))
        b.update(20.0)

        assertFailsWith<IllegalArgumentException> {
            a.merge(b.read())
        }
    }

    @Test
    fun `merge with identical relative error succeeds`() {
        val a = DDSketchStat(relativeError = 0.01, probabilities = doubleArrayOf(0.5))
        a.update(10.0)

        val b = DDSketchStat(relativeError = 0.01, probabilities = doubleArrayOf(0.5))
        b.update(20.0)

        a.merge(b.read())
        assertEquals(2.0, a.read().totalWeights, 1e-9)
    }

    @Test
    fun `merge with empty sketch is a no-op on existing data`() {
        val a = DDSketchStat(relativeError = 0.01, probabilities = doubleArrayOf(0.5))
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
            ),
        )

        assertEquals(10.0, a.read().totalWeights, 1e-9)
    }
}

class SketchProbabilitiesOwnershipTest {

    @Test
    fun `editing a DDSketch snapshot's probabilities does not relabel the stat`() {
        val s = DDSketchStat()
        repeat(1000) { s.update(it.toDouble()) }
        val before = s.read().quantiles[0]
        s.read().probabilities[0] = 0.999
        assertEquals(before, s.read().quantiles[0], DELTA)
    }

    @Test
    fun `editing a TDigest snapshot's probabilities does not relabel the stat`() {
        val s = TDigestStat()
        repeat(1000) { s.update(it.toDouble()) }
        val before = s.read().quantiles[0]
        s.read().probabilities[0] = 0.999
        assertEquals(before, s.read().quantiles[0], DELTA)
    }

    @Test
    fun `out-of-range values still count toward the rank`() {
        val sketch = DDSketchStat(relativeError = 0.01, probabilities = doubleArrayOf(0.5))
        sketch.update(Double.MIN_VALUE)
        sketch.update(1.0)
        sketch.update(Double.MAX_VALUE)
        val r = sketch.read()
        assertTrue(r.totalWeights == 3.0, "expected all three observations counted, got ${r.totalWeights}")
        assertTrue(r.quantiles[0] > 0.0 && r.quantiles[0].isFinite(), "p50 was ${r.quantiles[0]}")
    }
}
