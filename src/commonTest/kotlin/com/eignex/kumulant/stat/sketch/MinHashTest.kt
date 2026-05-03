package com.eignex.kumulant.stat.sketch

import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class MinHashTest {

    @Test
    fun `empty signature is all sentinels`() {
        val mh = MinHash(numHashes = 64)
        val r = mh.read()
        assertEquals(64, r.signatures.size)
        assertTrue(r.signatures.all { it == Long.MAX_VALUE })
        assertEquals(0L, r.totalSeen)
    }

    @Test
    fun `single update populates every slot`() {
        val mh = MinHash(numHashes = 64)
        mh.update(42L)
        val r = mh.read()
        assertTrue(r.signatures.all { it != Long.MAX_VALUE })
        assertEquals(1L, r.totalSeen)
    }

    @Test
    fun `identical sets give Jaccard 1`() {
        val a = MinHash(numHashes = 256, seed = 13L)
        val b = MinHash(numHashes = 256, seed = 13L)
        for (i in 0 until 1000) {
            a.update(i.toLong())
            b.update(i.toLong())
        }
        assertEquals(1.0, a.read().jaccard(b.read()), 1e-9)
    }

    @Test
    fun `disjoint sets give Jaccard near zero`() {
        val a = MinHash(numHashes = 256, seed = 21L)
        val b = MinHash(numHashes = 256, seed = 21L)
        for (i in 0 until 1000) a.update(i.toLong())
        for (i in 10_000 until 11_000) b.update(i.toLong())
        val j = a.read().jaccard(b.read())
        assertTrue(j < 0.05, "disjoint Jaccard=$j")
    }

    @Test
    fun `Jaccard estimate is close to true value`() {
        val numHashes = 512
        val a = MinHash(numHashes = numHashes, seed = 7L)
        val b = MinHash(numHashes = numHashes, seed = 7L)
        // Sets [0, 750) and [250, 1000): overlap=500, union=1000 → true Jaccard=0.5
        for (i in 0 until 750) a.update(i.toLong())
        for (i in 250 until 1000) b.update(i.toLong())
        val j = a.read().jaccard(b.read())
        // Standard error ≈ 1/sqrt(512) ≈ 0.044; allow 4σ.
        assertTrue(abs(j - 0.5) < 0.18, "Jaccard estimate=$j")
    }

    @Test
    fun `zero or negative weight is ignored`() {
        val mh = MinHash(numHashes = 16)
        mh.update(1L, weight = 0.0)
        mh.update(2L, weight = -1.0)
        val r = mh.read()
        assertEquals(0L, r.totalSeen)
        assertTrue(r.signatures.all { it == Long.MAX_VALUE })
    }

    @Test
    fun `merge combines two minhashes`() {
        val a = MinHash(numHashes = 128, seed = 5L)
        val b = MinHash(numHashes = 128, seed = 5L)
        for (i in 0 until 500) a.update(i.toLong())
        for (i in 500 until 1000) b.update(i.toLong())
        a.merge(b.read())

        val full = MinHash(numHashes = 128, seed = 5L)
        for (i in 0 until 1000) full.update(i.toLong())

        // Merge of disjoint streams is the elementwise min of two independent minhashes,
        // which equals the minhash over the union.
        assertTrue(a.read().signatures.contentEquals(full.read().signatures))
        assertEquals(1000L, a.read().totalSeen)
    }

    @Test
    fun `merge requires matching shape`() {
        val a = MinHash(numHashes = 64, seed = 1L)
        val bResult = MinHash(numHashes = 128, seed = 1L).read()
        assertFailsWith<IllegalArgumentException> { a.merge(bResult) }
        val cResult = MinHash(numHashes = 64, seed = 2L).read()
        assertFailsWith<IllegalArgumentException> { a.merge(cResult) }
    }

    @Test
    fun `reset clears state`() {
        val mh = MinHash(numHashes = 32)
        for (i in 1..100) mh.update(i.toLong())
        mh.reset()
        val r = mh.read()
        assertEquals(0L, r.totalSeen)
        assertTrue(r.signatures.all { it == Long.MAX_VALUE })
    }

    @Test
    fun `create produces independent stat`() {
        val a = MinHash(numHashes = 32, seed = 99L)
        for (i in 1..100) a.update(i.toLong())
        val b = a.create()
        assertEquals(0L, b.read().totalSeen)
        assertTrue(b.read().signatures.all { it == Long.MAX_VALUE })
        assertEquals(100L, a.read().totalSeen)
    }

    @Test
    fun `invalid args throw`() {
        assertFailsWith<IllegalArgumentException> { MinHash(numHashes = 0) }
        assertFailsWith<IllegalArgumentException> { MinHash(numHashes = -1) }
    }
}
