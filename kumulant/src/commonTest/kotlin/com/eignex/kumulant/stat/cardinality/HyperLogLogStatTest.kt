package com.eignex.kumulant.stat.cardinality

import com.eignex.kumulant.stream.splitmix64
import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class HyperLogLogStatTest {

    @Test
    fun `empty stat estimates zero`() {
        val hll = HyperLogLogStat(precision = 14)
        val r = hll.read()
        assertEquals(0.0, r.estimate)
        assertEquals(0L, r.totalSeen)
    }

    @Test
    fun `single key estimates near one`() {
        val hll = HyperLogLogStat(precision = 14)
        hll.update(42L)
        val r = hll.read()
        assertTrue(abs(r.estimate - 1.0) < 0.5, "estimate=${r.estimate}")
    }

    @Test
    fun `duplicate keys do not inflate estimate`() {
        val hll = HyperLogLogStat(precision = 14)
        repeat(1000) { hll.update(7L) }
        val r = hll.read()
        assertTrue(r.estimate < 2.0, "estimate=${r.estimate}")
    }

    @Test
    fun `1000 unique keys within expected error at precision 14`() {
        val hll = HyperLogLogStat(precision = 14)
        for (i in 1..1000) hll.update(i.toLong())
        val r = hll.read()
        val rel = abs(r.estimate - 1000.0) / 1000.0
        assertTrue(rel < 0.05, "estimate=${r.estimate} rel=$rel")
    }

    @Test
    fun `30000 unique keys within expected error at precision 14`() {
        val hll = HyperLogLogStat(precision = 14)
        for (i in 1..30_000) hll.update(i.toLong())
        val r = hll.read()
        val rel = abs(r.estimate - 30_000.0) / 30_000.0
        // Standard error is ~ 1.04 / sqrt(2^14) ~ 0.81%, allow 3sigma.
        assertTrue(rel < 0.03, "estimate=${r.estimate} rel=$rel")
    }

    @Test
    fun `accuracy across cardinalities including the medium range stays within 2 percent`() {
        // Sweeps the m..3m window at precision 14 (m=16384) where the unmodified Flajolet
        // estimator is classically prone to a few percent downward bias. SplitMix64
        // prehashing keeps the observed error inside ~1.4% across this range - well
        // under the 2% bound asserted here.
        val precision = 14
        val cardinalities = intArrayOf(1_000, 5_000, 10_000, 20_000, 30_000, 50_000)
        for (n in cardinalities) {
            val hll = HyperLogLogStat(precision = precision)
            for (i in 1..n) hll.update(i.toLong())
            val rel = abs(hll.read().estimate - n) / n
            assertTrue(rel < 0.02, "n=$n rel=$rel")
        }
    }

    @Test
    fun `merge of two halves matches full stream`() {
        val full = HyperLogLogStat(precision = 12)
        for (i in 1..20_000) full.update(i.toLong())

        val a = HyperLogLogStat(precision = 12)
        val b = HyperLogLogStat(precision = 12)
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
        val hll = HyperLogLogStat(precision = 8)
        for (i in 1..500) hll.update(i.toLong())
        hll.reset()
        val r = hll.read()
        assertEquals(0.0, r.estimate)
        assertEquals(0L, r.totalSeen)
    }

    @Test
    fun `create produces independent stat`() {
        val hll1 = HyperLogLogStat(precision = 10)
        val hll2 = hll1.create()
        for (i in 1..100) hll2.update(i.toLong())
        assertEquals(0.0, hll1.read().estimate)
        assertTrue(hll2.read().estimate > 50.0)
    }

    @Test
    fun `merge rejects mismatched precision`() {
        val a = HyperLogLogStat(precision = 10)
        val b = HyperLogLogStat(precision = 12)
        b.update(1L)
        assertFailsWith<IllegalArgumentException> { a.merge(b.read()) }
    }

    @Test
    fun `invalid precision throws`() {
        assertFailsWith<IllegalArgumentException> { HyperLogLogStat(precision = 3) }
        assertFailsWith<IllegalArgumentException> { HyperLogLogStat(precision = 19) }
    }

    @Test
    fun `zero weight update is ignored`() {
        val hll = HyperLogLogStat(precision = 8)
        hll.update(1L, weight = 0.0)
        assertEquals(0.0, hll.read().estimate)
    }

    @Test
    fun `splitmix-prehashed input gives same result as raw`() {
        // Sanity check: distribution quality is good for sequential input thanks to
        // internal splitmix; pre-hashing externally still yields a valid estimate.
        val raw = HyperLogLogStat(precision = 12)
        val hashed = HyperLogLogStat(precision = 12)
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
