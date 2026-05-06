package com.eignex.kumulant.stream

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

private const val DELTA = 1e-12

class SerialLongTest {

    @Test
    fun `load and store round-trip`() {
        val v = SerialLong(0L)
        v.store(42L)
        assertEquals(42L, v.load())
    }

    @Test
    fun `add mutates in place`() {
        val v = SerialLong(10L)
        v.add(5L)
        assertEquals(15L, v.load())
        v.add(-3L)
        assertEquals(12L, v.load())
    }

    @Test
    fun `addAndGet returns the updated value and not the delta`() {
        val v = SerialLong(10L)
        val returned = v.addAndGet(5L)
        assertEquals(15L, returned)
        assertEquals(15L, v.load())
    }

    @Test
    fun `getAndAdd returns the previous value`() {
        val v = SerialLong(10L)
        val returned = v.getAndAdd(5L)
        assertEquals(10L, returned)
        assertEquals(15L, v.load())
    }
}

class SerialDoubleTest {

    @Test
    fun `load and store round-trip`() {
        val v = SerialDouble(0.0)
        v.store(2.5)
        assertEquals(2.5, v.load(), DELTA)
    }

    @Test
    fun `add mutates in place`() {
        val v = SerialDouble(1.0)
        v.add(0.5)
        assertEquals(1.5, v.load(), DELTA)
    }

    @Test
    fun `addAndGet returns the updated value`() {
        val v = SerialDouble(1.0)
        assertEquals(3.5, v.addAndGet(2.5), DELTA)
        assertEquals(3.5, v.load(), DELTA)
    }

    @Test
    fun `getAndAdd returns the previous value`() {
        val v = SerialDouble(1.0)
        assertEquals(1.0, v.getAndAdd(2.5), DELTA)
        assertEquals(3.5, v.load(), DELTA)
    }
}

class SerialRefTest {

    @Test
    fun `load and store round-trip`() {
        val ref = SerialRef<String?>(null)
        ref.store("hello")
        assertEquals("hello", ref.load())
    }

    @Test
    fun `compareAndSet swaps only on identity match`() {
        val initial = "a"
        val ref = SerialRef(initial)
        assertTrue(ref.compareAndSet(initial, "b"))
        assertEquals("b", ref.load())
        assertFalse(ref.compareAndSet("a", "c"))
        assertEquals("b", ref.load())
    }

    @Test
    fun `compareAndExchange returns previous value on success`() {
        val initial = "a"
        val ref = SerialRef(initial)
        assertEquals("a", ref.compareAndExchange(initial, "b"))
        assertEquals("b", ref.load())
    }

    @Test
    fun `compareAndExchange returns current value on failure`() {
        val ref = SerialRef("a")
        assertEquals("a", ref.compareAndExchange("wrong", "b"))
        assertEquals("a", ref.load())
    }
}

class AtomicModeTest {

    @Test
    fun `AtomicDouble add is observable after return`() {
        val d = AtomicMode.newDouble(1.0)
        d.add(2.5)
        assertEquals(3.5, d.load(), DELTA)
    }

    @Test
    fun `AtomicDouble addAndGet returns updated value`() {
        val d = AtomicMode.newDouble(1.0)
        assertEquals(3.5, d.addAndGet(2.5), DELTA)
        assertEquals(3.5, d.load(), DELTA)
    }

    @Test
    fun `AtomicDouble getAndAdd returns previous value`() {
        val d = AtomicMode.newDouble(1.0)
        assertEquals(1.0, d.getAndAdd(2.5), DELTA)
        assertEquals(3.5, d.load(), DELTA)
    }

    @Test
    fun `AtomicDouble zero delta add is a no-op`() {
        val d = AtomicMode.newDouble(1.0)
        d.add(0.0)
        assertEquals(1.0, d.load(), DELTA)
    }

    @Test
    fun `AtomicLong addAndGet returns updated value`() {
        val l = AtomicMode.newLong(10L)
        assertEquals(13L, l.addAndGet(3L))
        assertEquals(13L, l.load())
    }

    @Test
    fun `AtomicLong getAndAdd returns previous value`() {
        val l = AtomicMode.newLong(10L)
        assertEquals(10L, l.getAndAdd(3L))
        assertEquals(13L, l.load())
    }

    @Test
    fun `AtomicReference compareAndSet swaps only on identity match`() {
        val initial = "a"
        val ref = AtomicMode.newReference(initial)
        assertTrue(ref.compareAndSet(initial, "b"))
        assertEquals("b", ref.load())
        assertFalse(ref.compareAndSet("a", "c"))
    }
}
