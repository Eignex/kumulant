package com.eignex.kumulant.stream

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

private const val DELTA = 1e-12

class AtomicDoubleTest {

    @Test
    fun `load and store round-trip including special values`() {
        val v = AtomicDouble(0.0)
        v.store(2.5)
        assertEquals(2.5, v.load(), DELTA)
        v.store(-0.0)
        assertEquals(-0.0, v.load(), DELTA)
        v.store(Double.POSITIVE_INFINITY)
        assertEquals(Double.POSITIVE_INFINITY, v.load())
    }

    @Test
    fun `add with zero delta short-circuits`() {
        val v = AtomicDouble(1.5)
        v.add(0.0)
        assertEquals(1.5, v.load(), DELTA)
    }

    @Test
    fun `add accumulates non-zero delta`() {
        val v = AtomicDouble(1.0)
        v.add(2.5)
        v.add(-1.5)
        assertEquals(2.0, v.load(), DELTA)
    }

    @Test
    fun `addAndGet returns the updated value`() {
        val v = AtomicDouble(10.0)
        assertEquals(12.5, v.addAndGet(2.5), DELTA)
        assertEquals(12.5, v.load(), DELTA)
    }

    @Test
    fun `compareAndSet swaps only when expected matches`() {
        val v = AtomicDouble(1.0)
        assertTrue(v.compareAndSet(1.0, 2.0))
        assertEquals(2.0, v.load(), DELTA)
        assertFalse(v.compareAndSet(1.0, 3.0))
        assertEquals(2.0, v.load(), DELTA)
    }
}

class AtomicLongModeTest {

    @Test
    fun `load and store round-trip`() {
        val v = AtomicLong(0L)
        v.store(123L)
        assertEquals(123L, v.load())
    }

    @Test
    fun `add and addAndGet update in place`() {
        val v = AtomicLong(10L)
        v.add(5L)
        assertEquals(15L, v.load())
        assertEquals(20L, v.addAndGet(5L))
    }

    @Test
    fun `compareAndSet swaps only when expected matches`() {
        val v = AtomicLong(7L)
        assertTrue(v.compareAndSet(7L, 8L))
        assertFalse(v.compareAndSet(7L, 9L))
        assertEquals(8L, v.load())
    }
}

class AtomicLongCellArrayTest {

    private fun arr(size: Int, init: (Int) -> Long) =
        AtomicMode.newLongArray(size, init)

    @Test
    fun `size and per-cell load reflect the initialiser`() {
        val a = arr(4) { it.toLong() * 10L }
        assertEquals(4, a.size)
        for (i in 0 until 4) assertEquals(i.toLong() * 10L, a.load(i))
    }

    @Test
    fun `store overwrites a cell`() {
        val a = arr(2) { 0L }
        a.store(1, 99L)
        assertEquals(0L, a.load(0))
        assertEquals(99L, a.load(1))
    }

    @Test
    fun `add and addAndGet update one cell`() {
        val a = arr(3) { 1L }
        a.add(0, 5L)
        assertEquals(6L, a.load(0))
        assertEquals(8L, a.addAndGet(1, 7L))
    }

    @Test
    fun `compareAndSet returns true on success and false on mismatch`() {
        val a = arr(1) { 4L }
        assertTrue(a.compareAndSet(0, 4L, 5L))
        assertFalse(a.compareAndSet(0, 4L, 6L))
        assertEquals(5L, a.load(0))
    }
}

class AtomicReferenceTest {

    @Test
    fun `load and store round-trip non-primitive references`() {
        val ref: StreamRef<String> = AtomicMode.newReference("hello")
        assertEquals("hello", ref.load())
        ref.store("world")
        assertEquals("world", ref.load())
    }

    @Test
    fun `compareAndSet swaps only on identity match`() {
        val ref = AtomicMode.newReference("a")
        val a = ref.load()
        assertTrue(ref.compareAndSet(a, "b"))
        assertFalse(ref.compareAndSet(a, "c"))
        assertEquals("b", ref.load())
    }

    @Test
    fun `compareAndExchange returns the witness`() {
        val ref = AtomicMode.newReference("x")
        val witness = ref.compareAndExchange("x", "y")
        assertEquals("x", witness)
        assertEquals("y", ref.load())
        val w2 = ref.compareAndExchange("x", "z")
        assertEquals("y", w2)
        assertEquals("y", ref.load())
    }

    @Test
    fun `newReference rejects boxed primitive payloads`() {
        assertFailsWith<IllegalArgumentException> { AtomicMode.newReference(1.0) }
        assertFailsWith<IllegalArgumentException> { AtomicMode.newReference(1L) }
    }
}
