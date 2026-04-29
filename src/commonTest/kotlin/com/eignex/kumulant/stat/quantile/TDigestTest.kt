package com.eignex.kumulant.stat.quantile

import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

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
