package com.eignex.kumulant.stat.cardinality

import com.eignex.kumulant.math.splitmix64
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
    fun `accuracy across cardinalities including the medium range stays within 2 percent`() {
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
    fun `splitmix-prehashed input gives same result as raw`() {
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
