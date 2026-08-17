package com.eignex.kumulant.stat.cardinality

import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class LinearCountingStatTest {

    @Test
    fun `empty stat estimates zero`() {
        val lc = LinearCountingStat(bits = 4096)
        val r = lc.read()
        assertEquals(0.0, r.estimate)
        assertEquals(4096L, r.unsetBits)
    }

    @Test
    fun `single key estimates near one`() {
        val lc = LinearCountingStat(bits = 4096)
        lc.update(42L)
        val r = lc.read()
        assertTrue(abs(r.estimate - 1.0) < 0.1, "estimate=${r.estimate}")
    }

    @Test
    fun `duplicate keys do not inflate estimate`() {
        val lc = LinearCountingStat(bits = 4096)
        repeat(1000) { lc.update(7L) }
        val r = lc.read()
        assertTrue(r.estimate < 2.0, "estimate=${r.estimate}")
    }

    @Test
    fun `100 unique keys within expected error`() {
        val lc = LinearCountingStat(bits = 16384)
        for (i in 1..100) lc.update(i.toLong())
        val r = lc.read()
        val rel = abs(r.estimate - 100.0) / 100.0
        assertTrue(rel < 0.05, "estimate=${r.estimate} rel=$rel")
    }

    @Test
    fun `saturated bitset returns positive infinity`() {
        val lc = LinearCountingStat(bits = 64)
        for (i in 1L..10_000L) lc.update(i)
        val r = lc.read()
        assertEquals(Double.POSITIVE_INFINITY, r.estimate)
        assertEquals(0L, r.unsetBits)
    }

    @Test
    fun `merge unions bitsets`() {
        val a = LinearCountingStat(bits = 4096)
        val b = LinearCountingStat(bits = 4096)
        for (i in 1..200) a.update(i.toLong())
        for (i in 201..400) b.update(i.toLong())

        a.merge(b.read())
        val merged = a.read().estimate
        val rel = abs(merged - 400.0) / 400.0
        assertTrue(rel < 0.1, "merged=$merged")
    }

    @Test
    fun `reset clears bitset and counter`() {
        val lc = LinearCountingStat(bits = 256)
        for (i in 1..50) lc.update(i.toLong())
        lc.reset()
        val r = lc.read()
        assertEquals(0.0, r.estimate)
        assertEquals(256L, r.unsetBits)
        assertEquals(0L, r.totalSeen)
    }

    @Test
    fun `create produces independent stat`() {
        val lc1 = LinearCountingStat(bits = 1024)
        val lc2 = lc1.create()
        for (i in 1..50) lc2.update(i.toLong())
        assertEquals(0.0, lc1.read().estimate)
        assertTrue(lc2.read().estimate > 30.0)
    }

    @Test
    fun `merge rejects mismatched size`() {
        val a = LinearCountingStat(bits = 1024)
        val b = LinearCountingStat(bits = 2048)
        b.update(1L)
        assertFailsWith<IllegalArgumentException> { a.merge(b.read()) }
    }

    @Test
    fun `invalid bits throws`() {
        assertFailsWith<IllegalArgumentException> { LinearCountingStat(bits = 0) }
        assertFailsWith<IllegalArgumentException> { LinearCountingStat(bits = 100) } // not a power of two
        assertFailsWith<IllegalArgumentException> { LinearCountingStat(bits = 32) } // not a multiple of 64
    }

    @Test
    fun `zero weight update is ignored`() {
        val lc = LinearCountingStat(bits = 256)
        lc.update(1L, weight = 0.0)
        assertEquals(0.0, lc.read().estimate)
    }
}
