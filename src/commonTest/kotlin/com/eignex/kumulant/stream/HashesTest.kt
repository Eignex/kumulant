package com.eignex.kumulant.stream

import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class SplitMix64Test {

    @Test
    fun `splitmix64 matches reference vectors`() {
        // Locked-in outputs — guard against accidental edits to the mixer constants.
        assertEquals(4553024054441242788L, splitmix64(0L))
        assertEquals(6615013642232309006L, splitmix64(1L))
        assertEquals(-2854182258122887073L, splitmix64(-1L))
        assertEquals(189591720424502296L, splitmix64(42L))
        assertEquals(2950148240053738312L, splitmix64(Long.MAX_VALUE))
        assertEquals(-6357312718828134429L, splitmix64(Long.MIN_VALUE))
    }

    @Test
    fun `splitmix64 produces approximately balanced bits across sequential inputs`() {
        val samples = 10_000
        val counts = IntArray(64)
        for (i in 0 until samples) {
            val h = splitmix64(i.toLong())
            for (b in 0 until 64) {
                if ((h ushr b) and 1L == 1L) counts[b]++
            }
        }
        for (b in 0 until 64) {
            val rate = counts[b].toDouble() / samples
            assertTrue(abs(rate - 0.5) < 0.05, "bit $b rate=$rate")
        }
    }
}

class Hash64Test {

    @Test
    fun `hash64 ByteArray is deterministic`() {
        val a = byteArrayOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        val b = byteArrayOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        assertEquals(hash64(a), hash64(b))
    }

    @Test
    fun `hash64 String matches encoded ByteArray`() {
        val s = "hello, hash"
        assertEquals(hash64(s.encodeToByteArray()), hash64(s))
    }

    @Test
    fun `hash64 distinguishes different lengths of zero-padded input`() {
        // Length is folded into the seed, so prefix-equal inputs of different
        // length must hash distinctly.
        val one = ByteArray(1)
        val two = ByteArray(2)
        val eight = ByteArray(8)
        assertNotEquals(hash64(one), hash64(two))
        assertNotEquals(hash64(two), hash64(eight))
        assertNotEquals(hash64(one), hash64(eight))
    }

    @Test
    fun `hash64 ByteArray matches reference vectors across platforms`() {
        // Locked-in outputs from SplitMixChunkHasher. Platform-stable; guards against
        // accidental endianness or chunking changes.
        assertEquals(0L, hash64(ByteArray(0)))
        assertEquals(6023229995862221380L, hash64("hello"))
        assertEquals(4208025021734807446L, hash64("the quick brown fox jumps over the lazy dog"))
        assertEquals(
            -9112064343569154153L,
            hash64(byteArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)),
        )
    }

    @Test
    fun `hash64 distribution is roughly uniform over sequential keys`() {
        val samples = 10_000
        val buckets = 16
        val counts = IntArray(buckets)
        for (i in 0 until samples) {
            val bytes = ByteArray(8) { byteIndex ->
                ((i.toLong() ushr (byteIndex * 8)) and 0xFF).toByte()
            }
            val h = hash64(bytes)
            counts[(h and (buckets - 1).toLong()).toInt()]++
        }
        val expected = samples.toDouble() / buckets
        for (b in 0 until buckets) {
            val ratio = counts[b] / expected
            assertTrue(ratio in 0.7..1.3, "bucket $b count=${counts[b]} ratio=$ratio")
        }
    }

    @Test
    fun `Hasher64 fun-interface accepts a lambda`() {
        val constant = Hasher64 { _ -> 42L }
        assertEquals(42L, constant.hash(byteArrayOf(1, 2, 3)))
    }

    @Test
    fun `SplitMixChunkHasher matches hash64 default`() {
        val sample = "the quick brown fox".encodeToByteArray()
        assertEquals(hash64(sample), SplitMixChunkHasher.hash(sample))
    }
}
