package com.eignex.kumulant.stat.cardinality

import com.eignex.kumulant.stream.splitmix64

import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class HyperLogLogTest {

    @Test
    fun `empty stat estimates zero`() {
        val hll = HyperLogLog(precision = 14)
        val r = hll.read()
        assertEquals(0.0, r.estimate)
        assertEquals(0L, r.totalSeen)
    }

    @Test
    fun `single key estimates near one`() {
        val hll = HyperLogLog(precision = 14)
        hll.update(42L)
        val r = hll.read()
        assertTrue(abs(r.estimate - 1.0) < 0.5, "estimate=${r.estimate}")
    }

    @Test
    fun `duplicate keys do not inflate estimate`() {
        val hll = HyperLogLog(precision = 14)
        repeat(1000) { hll.update(7L) }
        val r = hll.read()
        assertTrue(r.estimate < 2.0, "estimate=${r.estimate}")
    }

    @Test
    fun `1000 unique keys within expected error at precision 14`() {
        val hll = HyperLogLog(precision = 14)
        for (i in 1..1000) hll.update(i.toLong())
        val r = hll.read()
        val rel = abs(r.estimate - 1000.0) / 1000.0
        assertTrue(rel < 0.05, "estimate=${r.estimate} rel=$rel")
    }

    @Test
    fun `100000 unique keys within expected error at precision 14`() {
        val hll = HyperLogLog(precision = 14)
        for (i in 1..100_000) hll.update(i.toLong())
        val r = hll.read()
        val rel = abs(r.estimate - 100_000.0) / 100_000.0
        // Standard error is ≈ 1.04 / sqrt(2^14) ≈ 0.81%, allow 3σ.
        assertTrue(rel < 0.03, "estimate=${r.estimate} rel=$rel")
    }

    @Test
    fun `merge of two halves matches full stream`() {
        val full = HyperLogLog(precision = 12)
        for (i in 1..20_000) full.update(i.toLong())

        val a = HyperLogLog(precision = 12)
        val b = HyperLogLog(precision = 12)
        for (i in 1..10_000) a.update(i.toLong())
        for (i in 10_001..20_000) b.update(i.toLong())

        a.merge(b.read())
        val merged = a.read().estimate
        val direct = full.read().estimate
        val rel = abs(merged - direct) / direct
        assertTrue(rel < 0.02, "merged=$merged direct=$direct")
    }

    @Test
    fun `reset clears registers and counter`() {
        val hll = HyperLogLog(precision = 8)
        for (i in 1..500) hll.update(i.toLong())
        hll.reset()
        val r = hll.read()
        assertEquals(0.0, r.estimate)
        assertEquals(0L, r.totalSeen)
    }

    @Test
    fun `create produces independent stat`() {
        val hll1 = HyperLogLog(precision = 10)
        val hll2 = hll1.create()
        for (i in 1..100) hll2.update(i.toLong())
        assertEquals(0.0, hll1.read().estimate)
        assertTrue(hll2.read().estimate > 50.0)
    }

    @Test
    fun `merge rejects mismatched precision`() {
        val a = HyperLogLog(precision = 10)
        val b = HyperLogLog(precision = 12)
        b.update(1L)
        assertFailsWith<IllegalArgumentException> { a.merge(b.read()) }
    }

    @Test
    fun `invalid precision throws`() {
        assertFailsWith<IllegalArgumentException> { HyperLogLog(precision = 3) }
        assertFailsWith<IllegalArgumentException> { HyperLogLog(precision = 19) }
    }

    @Test
    fun `zero weight update is ignored`() {
        val hll = HyperLogLog(precision = 8)
        hll.update(1L, weight = 0.0)
        assertEquals(0.0, hll.read().estimate)
    }

    @Test
    fun `splitmix-prehashed input gives same result as raw`() {
        // Sanity check: distribution quality is good for sequential input thanks to
        // internal splitmix; pre-hashing externally still yields a valid estimate.
        val raw = HyperLogLog(precision = 12)
        val hashed = HyperLogLog(precision = 12)
        for (i in 1..5000) {
            raw.update(i.toLong())
            hashed.update(splitmix64(i.toLong()))
        }
        val rawEst = raw.read().estimate
        val hashedEst = hashed.read().estimate
        assertTrue(abs(rawEst - 5000.0) / 5000.0 < 0.05, "raw=$rawEst")
        assertTrue(abs(hashedEst - 5000.0) / 5000.0 < 0.05, "hashed=$hashedEst")
    }
}

class LinearCountingTest {

    @Test
    fun `empty stat estimates zero`() {
        val lc = LinearCounting(bits = 4096)
        val r = lc.read()
        assertEquals(0.0, r.estimate)
        assertEquals(4096L, r.unsetBits)
    }

    @Test
    fun `single key estimates near one`() {
        val lc = LinearCounting(bits = 4096)
        lc.update(42L)
        val r = lc.read()
        assertTrue(abs(r.estimate - 1.0) < 0.1, "estimate=${r.estimate}")
    }

    @Test
    fun `duplicate keys do not inflate estimate`() {
        val lc = LinearCounting(bits = 4096)
        repeat(1000) { lc.update(7L) }
        val r = lc.read()
        assertTrue(r.estimate < 2.0, "estimate=${r.estimate}")
    }

    @Test
    fun `100 unique keys within expected error`() {
        val lc = LinearCounting(bits = 16384)
        for (i in 1..100) lc.update(i.toLong())
        val r = lc.read()
        val rel = abs(r.estimate - 100.0) / 100.0
        assertTrue(rel < 0.05, "estimate=${r.estimate} rel=$rel")
    }

    @Test
    fun `saturated bitset returns positive infinity`() {
        val lc = LinearCounting(bits = 64)
        // Push enough distinct values to set every bit.
        for (i in 1L..100_000L) lc.update(i)
        val r = lc.read()
        assertEquals(Double.POSITIVE_INFINITY, r.estimate)
        assertEquals(0L, r.unsetBits)
    }

    @Test
    fun `merge unions bitsets`() {
        val a = LinearCounting(bits = 4096)
        val b = LinearCounting(bits = 4096)
        for (i in 1..200) a.update(i.toLong())
        for (i in 201..400) b.update(i.toLong())

        a.merge(b.read())
        val merged = a.read().estimate
        val rel = abs(merged - 400.0) / 400.0
        assertTrue(rel < 0.1, "merged=$merged")
    }

    @Test
    fun `reset clears bitset and counter`() {
        val lc = LinearCounting(bits = 256)
        for (i in 1..50) lc.update(i.toLong())
        lc.reset()
        val r = lc.read()
        assertEquals(0.0, r.estimate)
        assertEquals(256L, r.unsetBits)
        assertEquals(0L, r.totalSeen)
    }

    @Test
    fun `create produces independent stat`() {
        val lc1 = LinearCounting(bits = 1024)
        val lc2 = lc1.create()
        for (i in 1..50) lc2.update(i.toLong())
        assertEquals(0.0, lc1.read().estimate)
        assertTrue(lc2.read().estimate > 30.0)
    }

    @Test
    fun `merge rejects mismatched size`() {
        val a = LinearCounting(bits = 1024)
        val b = LinearCounting(bits = 2048)
        b.update(1L)
        assertFailsWith<IllegalArgumentException> { a.merge(b.read()) }
    }

    @Test
    fun `invalid bits throws`() {
        assertFailsWith<IllegalArgumentException> { LinearCounting(bits = 0) }
        assertFailsWith<IllegalArgumentException> { LinearCounting(bits = 100) }       // not a power of two
        assertFailsWith<IllegalArgumentException> { LinearCounting(bits = 32) }        // not a multiple of 64
    }

    @Test
    fun `zero weight update is ignored`() {
        val lc = LinearCounting(bits = 256)
        lc.update(1L, weight = 0.0)
        assertEquals(0.0, lc.read().estimate)
    }
}
