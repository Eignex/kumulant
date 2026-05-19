package com.eignex.kumulant.stat.sketch

import com.eignex.kumulant.core.Concurrency
import kotlin.test.Test
import kotlin.test.assertEquals

class SketchStatConcurrencyTest {

    private val keys = longArrayOf(1L, 2L, 3L, 2L, 1L, 4L, 5L, 1L, 6L, 7L)

    @Test
    fun `BloomFilterStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = BloomFilterStat(bits = 256, hashes = 4, concurrency = mode)
            for (k in keys) s.update(k)
            s.read(0L)
        }
        val ref = reads[Concurrency.None]!!
        for ((mode, r) in reads) {
            assertEquals(ref.totalSeen, r.totalSeen, "BloomFilter totalSeen mode=$mode")
            kotlin.test.assertTrue(ref.words.contentEquals(r.words), "BloomFilter words mode=$mode")
        }
    }

    @Test
    fun `CountMinSketchStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = CountMinSketchStat(depth = 4, width = 64, concurrency = mode)
            for (k in keys) s.update(k)
            s.read(0L)
        }
        val ref = reads[Concurrency.None]!!
        for ((mode, r) in reads) {
            // CountMinSketchResult has counters; their estimate equals across modes.
            assertEquals(ref.estimate(1L), r.estimate(1L), "CountMinSketch.estimate(1) mode=$mode")
            assertEquals(ref.estimate(2L), r.estimate(2L), "CountMinSketch.estimate(2) mode=$mode")
        }
    }

    @Test
    fun `MinHashStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = MinHashStat(numHashes = 32, concurrency = mode)
            for (k in keys) s.update(k)
            s.read(0L)
        }
        val ref = reads[Concurrency.None]!!
        for ((mode, r) in reads) {
            kotlin.test.assertTrue(ref.signatures.contentEquals(r.signatures), "MinHash signatures mode=$mode")
        }
    }

    @Test
    fun `SpaceSavingStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = SpaceSavingStat(capacity = 4, concurrency = mode)
            for (k in keys) s.update(k)
            s.read(0L)
        }
        val ref = reads[Concurrency.None]!!
        for ((mode, r) in reads) {
            assertEquals(ref.keys.toList(), r.keys.toList(), "SpaceSavingStat keys mode=$mode")
            assertEquals(ref.counts.toList(), r.counts.toList(), "SpaceSavingStat counts mode=$mode")
        }
    }
}
