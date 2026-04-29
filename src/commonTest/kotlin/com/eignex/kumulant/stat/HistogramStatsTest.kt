package com.eignex.kumulant.stat

import com.eignex.kumulant.core.QuantileResult
import com.eignex.kumulant.core.ReservoirResult
import com.eignex.kumulant.core.SparseHistogramResult
import com.eignex.kumulant.core.TDigestResult
import com.eignex.kumulant.core.quantile
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
        assertTrue(fq.read().quantile > 0.0)
    }

    @Test
    fun `estimate moves down when value is below it`() {
        val fq = FrugalQuantile(q = 0.5, stepSize = 1.0, initialEstimate = 100.0)
        fq.update(0.0)
        assertTrue(fq.read().quantile < 100.0)
    }

    @Test
    fun `estimate does not move when value equals it`() {
        val fq = FrugalQuantile(q = 0.5, stepSize = 1.0, initialEstimate = 5.0)
        fq.update(5.0)
        assertEquals(5.0, fq.read().quantile)
    }

    @Test
    fun `q=1 estimate only moves up`() {
        val fq = FrugalQuantile(q = 1.0, stepSize = 1.0, initialEstimate = 0.0)
        fq.update(50.0)
        val after = fq.read().quantile
        fq.update(0.0)
        assertEquals(after, fq.read().quantile)
    }

    @Test
    fun `q=0 estimate only moves down`() {
        val fq = FrugalQuantile(q = 0.0, stepSize = 1.0, initialEstimate = 100.0)
        fq.update(0.0)
        val after = fq.read().quantile
        fq.update(200.0)
        assertEquals(after, fq.read().quantile)
    }

    @Test
    fun `estimate stays near median of symmetric alternating stream`() {

        val fq = FrugalQuantile(q = 0.5, stepSize = 1.0, initialEstimate = 50.0)
        repeat(2000) {
            fq.update(0.0)
            fq.update(100.0)
        }

        assertTrue(fq.read().quantile in 40.0..60.0, "quantile=${fq.read().quantile}")
    }

    @Test
    fun `zero weight update is ignored`() {
        val fq = FrugalQuantile(q = 0.5, stepSize = 1.0, initialEstimate = 5.0)
        fq.update(100.0, weight = 0.0)
        assertEquals(5.0, fq.read().quantile)
    }

    @Test
    fun `reset returns to initial estimate`() {
        val fq = FrugalQuantile(q = 0.5, stepSize = 1.0, initialEstimate = 3.0)
        repeat(100) { fq.update(100.0) }
        fq.reset()
        assertEquals(3.0, fq.read().quantile)
    }

    @Test
    fun `merge averages two estimates`() {
        val fq = FrugalQuantile(q = 0.5, stepSize = 1.0, initialEstimate = 20.0)
        fq.merge(QuantileResult(0.5, 40.0))
        assertEquals(30.0, fq.read().quantile)
    }

    @Test
    fun `create produces fresh independent stat`() {
        val fq1 = FrugalQuantile(q = 0.5, stepSize = 1.0, initialEstimate = 0.0)
        val fq2 = fq1.create()
        repeat(100) { fq2.update(100.0) }
        assertEquals(0.0, fq1.read().quantile)
        assertTrue(fq2.read().quantile > fq1.read().quantile)
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

class ReservoirHistogramTest {

    @Test
    fun `empty reservoir`() {
        val r = ReservoirHistogram(capacity = 100).read()
        assertEquals(0, r.values.size)
        assertEquals(0L, r.totalSeen)
    }

    @Test
    fun `fills up to capacity then stays at capacity`() {
        val res = ReservoirHistogram(capacity = 10, seed = 42)
        for (i in 1..5) res.update(i.toDouble())
        assertEquals(5, res.read().values.size)
        for (i in 6..1000) res.update(i.toDouble())
        val r = res.read()
        assertEquals(10, r.values.size)
        assertEquals(1000L, r.totalSeen)
    }

    @Test
    fun `uniform stream sample median near population median`() {
        val res = ReservoirHistogram(capacity = 500, seed = 7)
        for (i in 1..10000) res.update(i.toDouble())
        val q = res.read().quantile(0.5)
        assertTrue(q in 4000.0..6000.0, "median=$q")
    }

    @Test
    fun `weighted sampling biases toward heavy weight`() {
        val res = ReservoirHistogram(capacity = 50, seed = 11)
        for (i in 1..1000) res.update(0.0, weight = 1.0)
        for (i in 1..1000) res.update(100.0, weight = 50.0)
        val mean = res.read().values.average()
        assertTrue(mean > 50.0, "expected heavy bias toward 100, got mean=$mean")
    }

    @Test
    fun `zero negative or NaN ignored`() {
        val res = ReservoirHistogram(capacity = 10, seed = 1)
        res.update(1.0, weight = 0.0)
        res.update(1.0, weight = -1.0)
        res.update(Double.NaN)
        assertEquals(0, res.read().values.size)
        assertEquals(0L, res.read().totalSeen)
    }

    @Test
    fun `reset clears state`() {
        val res = ReservoirHistogram(capacity = 10, seed = 0)
        for (i in 1..100) res.update(i.toDouble())
        res.reset()
        val r = res.read()
        assertEquals(0, r.values.size)
        assertEquals(0L, r.totalSeen)
    }

    @Test
    fun `create produces independent stat`() {
        val a = ReservoirHistogram(capacity = 10, seed = 5)
        for (i in 1..100) a.update(i.toDouble())
        val b = a.create()
        assertEquals(0, b.read().values.size)
        assertTrue(a.read().values.isNotEmpty())
    }

    @Test
    fun `deterministic with fixed seed`() {
        val a = ReservoirHistogram(capacity = 20, seed = 1234)
        val b = ReservoirHistogram(capacity = 20, seed = 1234)
        for (i in 1..500) {
            a.update(i.toDouble())
            b.update(i.toDouble())
        }
        assertTrue(a.read().values.contentEquals(b.read().values))
    }

    @Test
    fun `merge combines two reservoirs`() {
        val a = ReservoirHistogram(capacity = 50, seed = 1)
        val b = ReservoirHistogram(capacity = 50, seed = 2)
        for (i in 1..200) a.update(i.toDouble())
        for (i in 201..400) b.update(i.toDouble())
        a.merge(b.read())
        val r = a.read()
        assertEquals(50, r.values.size)
        assertEquals(400L, r.totalSeen)
        assertTrue(r.values.any { it > 200.0 }, "merged sample should include upper half")
    }

    @Test
    fun `invalid capacity throws`() {
        assertFailsWith<IllegalArgumentException> { ReservoirHistogram(capacity = 0) }
        assertFailsWith<IllegalArgumentException> { ReservoirHistogram(capacity = -1) }
    }
}

class TDigestTest {

    @Test
    fun `empty digest returns zero quantiles`() {
        val td = TDigest(probabilities = doubleArrayOf(0.5, 0.9))
        val r = td.read()
        assertEquals(0.0, r.quantiles[0])
        assertEquals(0.0, r.quantiles[1])
        assertEquals(0.0, r.totalWeight)
    }

    @Test
    fun `single value returns that value`() {
        val td = TDigest(probabilities = doubleArrayOf(0.1, 0.5, 0.9))
        td.update(42.0)
        val r = td.read()
        for (q in r.quantiles) assertEquals(42.0, q, 1e-9)
    }

    @Test
    fun `p50 of 1 to 100 is near 50`() {
        val td = TDigest(probabilities = doubleArrayOf(0.5))
        for (i in 1..100) td.update(i.toDouble())
        val q = td.read().quantiles[0]
        assertTrue(q in 47.0..53.0, "p50=$q")
    }

    @Test
    fun `p90 of 1 to 1000 is near 900`() {
        val td = TDigest(probabilities = doubleArrayOf(0.9))
        for (i in 1..1000) td.update(i.toDouble())
        val q = td.read().quantiles[0]
        assertTrue(q in 880.0..920.0, "p90=$q")
    }

    @Test
    fun `extreme quantiles are accurate`() {
        val td = TDigest(probabilities = doubleArrayOf(0.99, 0.999))
        for (i in 1..10000) td.update(i.toDouble())
        val r = td.read()
        assertTrue(abs(r.quantiles[0] - 9900.0) < 100.0, "p99=${r.quantiles[0]}")
        assertTrue(abs(r.quantiles[1] - 9990.0) < 50.0, "p999=${r.quantiles[1]}")
    }

    @Test
    fun `quantiles are monotonic`() {
        val td = TDigest(
            probabilities = doubleArrayOf(0.1, 0.25, 0.5, 0.75, 0.9, 0.99)
        )
        for (i in 1..1000) td.update(i.toDouble())
        val qs = td.read().quantiles
        for (i in 0 until qs.size - 1) {
            assertTrue(qs[i] <= qs[i + 1], "non-monotonic: ${qs.toList()}")
        }
    }

    @Test
    fun `handles negative values`() {
        val td = TDigest(probabilities = doubleArrayOf(0.5))
        for (i in -50..-1) td.update(i.toDouble())
        for (i in 1..50) td.update(i.toDouble())
        val q = td.read().quantiles[0]
        assertTrue(q in -5.0..5.0, "p50=$q")
    }

    @Test
    fun `weighted updates shift quantile`() {
        val td = TDigest(probabilities = doubleArrayOf(0.5))
        for (i in 1..50) td.update(i.toDouble(), weight = 1.0)
        for (i in 51..100) td.update(i.toDouble(), weight = 5.0)
        val q = td.read().quantiles[0]
        assertTrue(q > 60.0, "weighted p50 should shift up, got $q")
    }

    @Test
    fun `zero weight ignored`() {
        val td = TDigest(probabilities = doubleArrayOf(0.5))
        td.update(100.0, weight = 0.0)
        td.update(Double.NaN)
        assertEquals(0.0, td.read().totalWeight)
    }

    @Test
    fun `centroid count bounded by compression`() {
        val compression = 100.0
        val td = TDigest(compression = compression, probabilities = doubleArrayOf(0.5))
        for (i in 1..50000) td.update(i.toDouble())
        val n = td.read().means.size
        assertTrue(n <= 6 * compression, "centroid count $n exceeded ~6×δ bound")
    }

    @Test
    fun `merge combines two digests`() {
        val a = TDigest(probabilities = doubleArrayOf(0.5))
        val b = TDigest(probabilities = doubleArrayOf(0.5))
        for (i in 1..50) a.update(i.toDouble())
        for (i in 51..100) b.update(i.toDouble())
        a.merge(b.read())
        val r = a.read()
        assertEquals(100.0, r.totalWeight, 1e-9)
        assertTrue(r.quantiles[0] in 47.0..53.0, "merged p50=${r.quantiles[0]}")
    }

    @Test
    fun `merge rejects mismatched compression`() {
        val a = TDigest(compression = 100.0)
        val bResult = TDigest(compression = 50.0).read()
        assertFailsWith<IllegalArgumentException> { a.merge(bResult) }
    }

    @Test
    fun `reset clears state`() {
        val td = TDigest()
        for (i in 1..100) td.update(i.toDouble())
        td.reset()
        val r = td.read()
        assertEquals(0.0, r.totalWeight)
        assertEquals(0, r.means.size)
    }

    @Test
    fun `create produces independent stat`() {
        val a = TDigest()
        for (i in 1..100) a.update(i.toDouble())
        val b = a.create()
        assertEquals(0.0, b.read().totalWeight)
        assertEquals(100.0, a.read().totalWeight, 1e-9)
    }

    @Test
    fun `invalid compression throws`() {
        assertFailsWith<IllegalArgumentException> { TDigest(compression = 0.0) }
        assertFailsWith<IllegalArgumentException> { TDigest(compression = -1.0) }
    }
}

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
        assertEquals(1.0, r.weights[0], 1e-9)
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
        assertEquals(10.0, r.weights[0], 1e-9)
    }

    @Test
    fun `weights accumulate correctly`() {
        val h = HdrHistogram()
        h.update(5.0, weight = 2.0)
        h.update(5.0, weight = 3.0)
        val r = h.read()
        assertEquals(5.0, r.weights.sum(), 1e-9)
    }

    @Test
    fun `negative values are silently ignored`() {
        val h = HdrHistogram()
        h.update(-1.0)
        h.update(5.0)
        val r = h.read()
        assertEquals(1.0, r.weights.sum(), 1e-9)
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
        h.update(5000.0)
        val r = h.read()
        assertEquals(2.0, r.weights.sum(), 1e-9)
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
        assertEquals(4.0, merged.weights.sum(), 1e-9)
    }

    @Test
    fun `merge with empty source is a no-op`() {
        val h1 = HdrHistogram()
        h1.update(5.0)
        val before = h1.read().weights.sum()

        h1.merge(com.eignex.kumulant.core.SparseHistogramResult(DoubleArray(0), DoubleArray(0), DoubleArray(0)))
        assertEquals(before, h1.read().weights.sum(), 1e-9)
    }

    @Test
    fun `create returns an independent histogram`() {
        val h1 = HdrHistogram()
        h1.update(5.0)
        val h2 = h1.create()
        repeat(10) { h2.update(5.0) }
        assertEquals(1.0, h1.read().weights.sum(), 1e-9)
        assertEquals(10.0, h2.read().weights.sum(), 1e-9)
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
        assertEquals(1.0, r.weights.sum(), 1e-9)
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
            com.eignex.kumulant.core.SketchResult(
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
