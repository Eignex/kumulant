package com.eignex.kumulant.stat.quantile

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ReservoirHistogramTest {

    @Test
    fun `empty reservoir`() {
        val r = ReservoirHistogramStat(capacity = 100).read()
        assertEquals(0, r.values.size)
        assertEquals(0L, r.totalSeen)
    }

    @Test
    fun `fills up to capacity then stays at capacity`() {
        val res = ReservoirHistogramStat(capacity = 10, seed = 42)
        for (i in 1..5) res.update(i.toDouble())
        assertEquals(5, res.read().values.size)
        for (i in 6..1000) res.update(i.toDouble())
        val r = res.read()
        assertEquals(10, r.values.size)
        assertEquals(1000L, r.totalSeen)
    }

    @Test
    fun `uniform stream sample median near population median`() {
        val res = ReservoirHistogramStat(capacity = 500, seed = 7)
        for (i in 1..10000) res.update(i.toDouble())
        val q = res.read().quantile(0.5)
        assertTrue(q in 4000.0..6000.0, "median=$q")
    }

    @Test
    fun `weighted sampling biases toward heavy weight`() {
        val res = ReservoirHistogramStat(capacity = 50, seed = 11)
        repeat(1000) { res.update(0.0, weight = 1.0) }
        repeat(1000) { res.update(100.0, weight = 50.0) }
        val mean = res.read().values.average()
        assertTrue(mean > 50.0, "expected heavy bias toward 100, got mean=$mean")
    }

    @Test
    fun `an inert or negative weight is ignored`() {
        val res = ReservoirHistogramStat(capacity = 10, seed = 1)
        res.update(1.0, weight = 0.0)
        res.update(1.0, weight = -1.0)
        res.update(1.0, weight = Double.NaN)
        assertEquals(0, res.read().values.size)
        assertEquals(0L, res.read().totalSeen)
    }

    @Test
    fun `a NaN value is sampled like any other rather than ignored`() {
        // A NaN value is a real observation; the reservoir stores it and the caller filters upstream
        // if that is unwanted. See Stat for the contract and DDSketchNaNTest for the filter.
        val res = ReservoirHistogramStat(capacity = 10, seed = 1)

        res.update(Double.NaN)

        assertEquals(1, res.read().values.size, "a NaN value must not be silently discarded")
        assertEquals(1L, res.read().totalSeen)
    }

    @Test
    fun `reset clears state`() {
        val res = ReservoirHistogramStat(capacity = 10, seed = 0)
        for (i in 1..100) res.update(i.toDouble())
        res.reset()
        val r = res.read()
        assertEquals(0, r.values.size)
        assertEquals(0L, r.totalSeen)
    }

    @Test
    fun `create produces independent stat`() {
        val a = ReservoirHistogramStat(capacity = 10, seed = 5)
        for (i in 1..100) a.update(i.toDouble())
        val b = a.create()
        assertEquals(0, b.read().values.size)
        assertTrue(a.read().values.isNotEmpty())
    }

    @Test
    fun `copies draw independent admission streams`() {
        // A windowed reservoir builds one slice per copy. Sharing the parent's seed would give every
        // slice the same admission keys in the same order, so surviving would turn on an
        // observation's position within its slice rather than on its weight.
        val template = ReservoirHistogramStat(capacity = 8, seed = 99)
        val first = template.create()
        val second = template.create()
        for (i in 1..200) {
            first.update(i.toDouble())
            second.update(i.toDouble())
        }
        assertFalse(first.read().values.contentEquals(second.read().values))
    }

    @Test
    fun `deterministic with fixed seed`() {
        val a = ReservoirHistogramStat(capacity = 20, seed = 1234)
        val b = ReservoirHistogramStat(capacity = 20, seed = 1234)
        for (i in 1..500) {
            a.update(i.toDouble())
            b.update(i.toDouble())
        }
        assertTrue(a.read().values.contentEquals(b.read().values))
    }

    @Test
    fun `merge combines two reservoirs`() {
        val a = ReservoirHistogramStat(capacity = 50, seed = 1)
        val b = ReservoirHistogramStat(capacity = 50, seed = 2)
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
        assertFailsWith<IllegalArgumentException> { ReservoirHistogramStat(capacity = 0) }
        assertFailsWith<IllegalArgumentException> { ReservoirHistogramStat(capacity = -1) }
    }

    @Test
    fun `toSparseHistogram on empty reservoir returns empty arrays`() {
        val r = ReservoirHistogramStat(capacity = 10).read()
        val h = r.toSparseHistogram(binCount = 5)
        assertEquals(0, h.lowerBounds.size)
        assertEquals(0, h.upperBounds.size)
        assertEquals(0, h.weights.size)
    }

    @Test
    fun `toSparseHistogram with a single distinct value collapses to one bin`() {
        val res = ReservoirHistogramStat(capacity = 10, seed = 1)
        repeat(5) { res.update(7.0) }
        val h = res.read().toSparseHistogram(binCount = 4)
        assertEquals(1, h.lowerBounds.size)
        assertEquals(7.0, h.lowerBounds[0])
        // upper must be strictly greater than lower so the half-open [lower, upper)
        // interval actually contains the sampled value.
        assertTrue(h.upperBounds[0] > h.lowerBounds[0])
        assertEquals(5.0, h.weights[0])
    }

    @Test
    fun `toSparseHistogram buckets distinct values into populated bins only`() {
        val res = ReservoirHistogramStat(capacity = 10, seed = 1)
        listOf(0.0, 1.0, 2.0, 3.0).forEach { res.update(it) }
        val h = res.read().toSparseHistogram(binCount = 4)
        // 4 values spread across [0, 3] into 4 bins of width 0.75 - every bin populated.
        assertEquals(4, h.weights.size)
        assertEquals(4.0, h.weights.sum())
    }

    @Test
    fun `toSparseHistogram rejects non-positive bin count`() {
        val r = ReservoirHistogramStat(capacity = 10).read()
        assertFailsWith<IllegalArgumentException> { r.toSparseHistogram(binCount = 0) }
    }

    @Test
    fun `quantile rejects out-of-range probability`() {
        val r = ReservoirHistogramStat(capacity = 10).read()
        assertFailsWith<IllegalArgumentException> { r.quantile(-0.1) }
        assertFailsWith<IllegalArgumentException> { r.quantile(1.1) }
    }

    @Test
    fun `quantile on empty reservoir returns NaN`() {
        val r = ReservoirHistogramStat(capacity = 10).read()
        assertTrue(r.quantile(0.5).isNaN())
    }

    @Test
    fun `quantile on a single-value reservoir returns that value`() {
        val res = ReservoirHistogramStat(capacity = 10, seed = 1)
        res.update(42.0)
        assertEquals(42.0, res.read().quantile(0.5))
    }
}
