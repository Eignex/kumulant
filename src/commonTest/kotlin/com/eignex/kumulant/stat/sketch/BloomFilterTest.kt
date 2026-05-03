package com.eignex.kumulant.stat.sketch

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class BloomFilterTest {

    @Test
    fun `empty filter contains nothing`() {
        val bf = BloomFilter(bits = 4096, hashes = 4)
        val r = bf.read()
        assertEquals(0L, r.totalSeen)
        assertFalse(r.contains(0L))
        assertFalse(r.contains(42L))
    }

    @Test
    fun `inserted key is contained`() {
        val bf = BloomFilter(bits = 4096, hashes = 4)
        bf.update(42L)
        val r = bf.read()
        assertTrue(r.contains(42L))
        assertEquals(1L, r.totalSeen)
    }

    @Test
    fun `no false negatives across many inserts`() {
        val bf = BloomFilter(bits = 1 shl 14, hashes = 7)
        for (i in 0 until 1000) bf.update(i.toLong())
        val r = bf.read()
        for (i in 0 until 1000) {
            assertTrue(r.contains(i.toLong()), "missing inserted key $i")
        }
    }

    @Test
    fun `false positive rate is below the theoretical bound`() {
        val bits = 4096
        val hashes = 4
        val bf = BloomFilter(bits = bits, hashes = hashes)
        for (i in 0 until 1000) bf.update(i.toLong())
        val r = bf.read()
        var fps = 0
        val queries = 10_000
        for (i in 1_000_000 until 1_000_000 + queries) {
            if (r.contains(i.toLong())) fps++
        }
        val rate = fps.toDouble() / queries
        // Theoretical FP ≈ (1 - e^(-4*1000/4096))^4 ≈ 0.24 — keep slack.
        assertTrue(rate < 0.32, "FP rate=$rate")
    }

    @Test
    fun `zero or negative weight is ignored`() {
        val bf = BloomFilter(bits = 4096, hashes = 4)
        bf.update(1L, weight = 0.0)
        bf.update(2L, weight = -1.0)
        val r = bf.read()
        assertEquals(0L, r.totalSeen)
        assertFalse(r.contains(1L))
        assertFalse(r.contains(2L))
    }

    @Test
    fun `merge combines two filters`() {
        val a = BloomFilter(bits = 4096, hashes = 4)
        val b = BloomFilter(bits = 4096, hashes = 4)
        for (i in 0 until 100) a.update(i.toLong())
        for (i in 100 until 200) b.update(i.toLong())
        a.merge(b.read())
        val r = a.read()
        assertEquals(200L, r.totalSeen)
        for (i in 0 until 200) assertTrue(r.contains(i.toLong()), "missing $i")
    }

    @Test
    fun `merge requires matching shape`() {
        val a = BloomFilter(bits = 4096, hashes = 4)
        val bResult = BloomFilter(bits = 8192, hashes = 4).read()
        assertFailsWith<IllegalArgumentException> { a.merge(bResult) }
        val cResult = BloomFilter(bits = 4096, hashes = 5).read()
        assertFailsWith<IllegalArgumentException> { a.merge(cResult) }
    }

    @Test
    fun `reset clears state`() {
        val bf = BloomFilter(bits = 4096, hashes = 4)
        for (i in 0 until 100) bf.update(i.toLong())
        bf.reset()
        val r = bf.read()
        assertEquals(0L, r.totalSeen)
        for (i in 0 until 100) assertFalse(r.contains(i.toLong()))
    }

    @Test
    fun `create produces independent stat`() {
        val a = BloomFilter(bits = 4096, hashes = 4)
        a.update(7L)
        val b = a.create()
        b.update(8L)
        assertTrue(a.read().contains(7L))
        assertFalse(a.read().contains(8L))
        assertTrue(b.read().contains(8L))
        assertFalse(b.read().contains(7L))
    }

    @Test
    fun `invalid args throw`() {
        assertFailsWith<IllegalArgumentException> { BloomFilter(bits = 0, hashes = 4) }
        assertFailsWith<IllegalArgumentException> { BloomFilter(bits = 1000, hashes = 4) }
        assertFailsWith<IllegalArgumentException> { BloomFilter(bits = 32, hashes = 4) }
        assertFailsWith<IllegalArgumentException> { BloomFilter(bits = 4096, hashes = 0) }
    }
}
