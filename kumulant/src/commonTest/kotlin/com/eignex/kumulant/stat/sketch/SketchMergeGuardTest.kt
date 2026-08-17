package com.eignex.kumulant.stat.sketch

import com.eignex.kumulant.math.Hashers
import com.eignex.kumulant.math.LongHasher
import com.eignex.kumulant.stat.cardinality.HyperLogLogStat
import com.eignex.kumulant.stat.cardinality.LinearCountingStat
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

// The mixer decides which cell a key touches, so once the cells disagree the merged counts are
// meaningless in a plausible-looking way rather than an obviously wrong one.
class SketchMergeGuardTest {

    /** A second registered mixer, distinguishable from the default only by name. */
    private object OtherHasher : LongHasher {
        override val name: String = "zz-other-mixer"
        override fun mix(value: Long): Long {
            var z = value + -0x61c8864680b583ebL
            z = (z xor (z ushr 30)) * -0x40a7b892e31b1a47L
            z = (z xor (z ushr 27)) * -0x6b2fb644ecceee15L
            return z xor (z ushr 31)
        }
    }

    @BeforeTest
    fun registerHasher() {
        Hashers.register(OtherHasher)
    }

    @Test
    fun `a Bloom filter refuses a merge from a different mixer`() {
        val other = BloomFilterStat(bits = 1024, hashes = 7, hasher = OtherHasher)
        other.update(42L)
        val receiver = BloomFilterStat(bits = 1024, hashes = 7)

        // Accepting this relabels the incoming bits as the receiver's, producing a false negative -
        // the one thing a Bloom filter promises cannot happen.
        assertFailsWith<IllegalArgumentException> { receiver.merge(other.read()) }
    }

    @Test
    fun `a Bloom filter still merges from its own mixer`() {
        val source = BloomFilterStat(bits = 1024, hashes = 7)
        source.update(42L)
        val receiver = BloomFilterStat(bits = 1024, hashes = 7)

        receiver.merge(source.read())

        assertTrue(receiver.read().contains(42L), "a same-mixer merge must preserve membership")
    }

    @Test
    fun `a count-min sketch refuses a merge from a different mixer`() {
        val other = CountMinSketchStat(depth = 5, width = 1024, hasher = OtherHasher)
        repeat(100) { other.update(42L) }
        val receiver = CountMinSketchStat(depth = 5, width = 1024)

        // Accepting this puts the counts in cells the receiver never probes, so estimate() falls
        // below the true count and breaks the one-sided guarantee.
        assertFailsWith<IllegalArgumentException> { receiver.merge(other.read()) }
    }

    @Test
    fun `a count-min sketch still merges from its own mixer`() {
        val source = CountMinSketchStat(depth = 5, width = 1024)
        repeat(100) { source.update(42L) }
        val receiver = CountMinSketchStat(depth = 5, width = 1024)

        receiver.merge(source.read())

        assertTrue(receiver.read().estimate(42L) >= 100L, "a same-mixer merge must keep the count")
    }

    @Test
    fun `HyperLogLog LinearCounting and MinHash refuse a merge from a different mixer`() {
        // The mixer has to travel on the wire, or the mismatch cannot be detected at all: a union of
        // two identical 10 000-key sets estimates ~19 620.
        val hll = HyperLogLogStat(precision = 12)
        assertFailsWith<IllegalArgumentException> {
            hll.merge(HyperLogLogStat(precision = 12, hasher = OtherHasher).also { it.update(1L) }.read())
        }
        val lc = LinearCountingStat(bits = 4096)
        assertFailsWith<IllegalArgumentException> {
            lc.merge(LinearCountingStat(bits = 4096, hasher = OtherHasher).also { it.update(1L) }.read())
        }
        val mh = MinHashStat(numHashes = 64)
        assertFailsWith<IllegalArgumentException> {
            mh.merge(MinHashStat(numHashes = 64, hasher = OtherHasher).also { it.update(1L) }.read())
        }
    }

    @Test
    fun `a same-mixer union stays close to the true cardinality`() {
        val a = HyperLogLogStat(precision = 12)
        val b = HyperLogLogStat(precision = 12)
        for (i in 0L until 10_000L) {
            a.update(i)
            b.update(i)
        }
        val union = HyperLogLogStat(precision = 12)

        union.merge(a.read())
        union.merge(b.read())

        // Merging the same 10 000 keys twice is still 10 000 distinct keys.
        val estimate = union.read().estimate
        assertTrue(estimate in 9_000.0..11_000.0, "union of identical sets estimated $estimate, expected ~10000")
    }

    @Test
    fun `a truncated payload is refused by name rather than throwing from inside merge`() {
        val full = CountMinSketchStat(depth = 5, width = 1024)
        full.update(1L)
        val snapshot = full.read()
        val truncated = snapshot.copy(counters = snapshot.counters.copyOf(10))

        val error = assertFailsWith<IllegalArgumentException> {
            CountMinSketchStat(depth = 5, width = 1024).merge(truncated)
        }
        assertTrue(
            error.message?.contains("counters") == true,
            "expected a message naming the payload, got ${error.message}",
        )
    }

    @Test
    fun `a huge weight neither reports zero nor wraps negative`() {
        val cms = CountMinSketchStat(depth = 3, width = 16)

        cms.update(1L, weight = 1e19)
        val once = cms.read().estimate(1L)
        cms.update(1L, weight = 1e19)
        val twice = cms.read().estimate(1L)

        assertTrue(once > 0L, "a saturating weight reported $once")
        assertTrue(twice >= once, "a second saturating update wrapped: $once then $twice")
    }

    @Test
    fun `a count-min sketch shape that overflows Int is rejected at construction`() {
        // depth * width is the array length and is computed in Int.
        assertFailsWith<IllegalArgumentException> { CountMinSketchStat(depth = 16, width = 1 shl 28) }
    }

    @Test
    fun `a fractional weight is counted rather than dropped`() {
        val cms = CountMinSketchStat(depth = 3, width = 64)

        repeat(10) { cms.update(42L, weight = 0.4) }

        val r = cms.read()
        assertEquals(10L, r.totalSeen, "totalSeen must count observations the sketch actually saw")
        assertTrue(r.estimate(42L) >= 4L, "true weight was 4.0 but the estimate is ${r.estimate(42L)}")
    }
}
