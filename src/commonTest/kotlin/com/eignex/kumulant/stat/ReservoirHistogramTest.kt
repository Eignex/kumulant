package com.eignex.kumulant.stat

import com.eignex.kumulant.core.quantile
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

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
