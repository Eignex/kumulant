package com.eignex.kumulant.stat.sketch

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class SpaceSavingTest {

    @Test
    fun `empty snapshot is well-formed`() {
        val ss = SpaceSaving(capacity = 10)
        val r = ss.read()
        assertEquals(10, r.capacity)
        assertEquals(0, r.keys.size)
        assertEquals(0, r.counts.size)
        assertEquals(0, r.errors.size)
        assertEquals(0L, r.totalSeen)
    }

    @Test
    fun `single update populates one slot`() {
        val ss = SpaceSaving(capacity = 10)
        ss.update(42L)
        val r = ss.read()
        assertEquals(1, r.keys.size)
        assertEquals(42L, r.keys[0])
        assertEquals(1L, r.counts[0])
        assertEquals(0L, r.errors[0])
        assertEquals(1L, r.totalSeen)
    }

    @Test
    fun `repeated key accumulates count without error`() {
        val ss = SpaceSaving(capacity = 10)
        repeat(7) { ss.update(42L) }
        val r = ss.read()
        assertEquals(1, r.keys.size)
        assertEquals(7L, r.counts[0])
        assertEquals(0L, r.errors[0])
    }

    @Test
    fun `fills up to capacity then evicts min`() {
        val ss = SpaceSaving(capacity = 3)
        ss.update(1L)
        ss.update(1L)
        ss.update(1L) // 1: count=3
        ss.update(2L)
        ss.update(2L) // 2: count=2
        ss.update(3L) // 3: count=1
        // 4 arrives, evicts 3 (the min, count=1): 4 -> count=1+1=2, error=1
        ss.update(4L)
        val r = ss.read()
        val kv = r.keys.zip(r.counts.toList()).toMap()
        assertEquals(3, kv.size)
        assertTrue(1L in kv.keys && 2L in kv.keys && 4L in kv.keys)
        assertTrue(3L !in kv.keys)
        assertEquals(3L, kv[1L])
        assertEquals(2L, kv[2L])
        assertEquals(2L, kv[4L])
        val errMap = r.keys.zip(r.errors.toList()).toMap()
        assertEquals(1L, errMap[4L])
        assertEquals(0L, errMap[1L])
        assertEquals(0L, errMap[2L])
    }

    @Test
    fun `recovers heavy hitters of a skewed stream`() {
        val ss = SpaceSaving(capacity = 16)
        // Hot keys 0..3 each occur 1000 times; cold keys 100..1099 each occur once.
        val hot = (0L..3L).toList()
        for (k in hot) repeat(1000) { ss.update(k) }
        for (i in 100L..1099L) ss.update(i)
        val r = ss.read()
        val keys = r.keys.toSet()
        for (k in hot) assertTrue(k in keys, "hot key $k missing")
        // Reported counts never undershoot true counts.
        for (i in r.keys.indices) {
            val k = r.keys[i]
            val trueCount = if (k in hot) 1000L else 1L
            assertTrue(r.counts[i] >= trueCount, "count($k)=${r.counts[i]} < true=$trueCount")
        }
    }

    @Test
    fun `weighted update accumulates`() {
        val ss = SpaceSaving(capacity = 5)
        ss.update(7L, weight = 5.0)
        ss.update(7L, weight = 3.0)
        val r = ss.read()
        assertEquals(8L, r.counts[0])
    }

    @Test
    fun `zero or negative weight is ignored`() {
        val ss = SpaceSaving(capacity = 5)
        ss.update(1L, weight = 0.0)
        ss.update(1L, weight = -1.0)
        val r = ss.read()
        assertEquals(0, r.keys.size)
        assertEquals(0L, r.totalSeen)
    }

    @Test
    fun `merge combines two summaries`() {
        val a = SpaceSaving(capacity = 10)
        val b = SpaceSaving(capacity = 10)
        repeat(5) { a.update(1L) }
        repeat(3) { a.update(2L) }
        repeat(7) { b.update(2L) }
        repeat(2) { b.update(3L) }
        a.merge(b.read())
        val r = a.read()
        val kv = r.keys.zip(r.counts.toList()).toMap()
        assertEquals(5L, kv[1L])
        assertEquals(10L, kv[2L])
        assertEquals(2L, kv[3L])
        assertEquals(17L, r.totalSeen)
    }

    @Test
    fun `merge requires matching capacity`() {
        val a = SpaceSaving(capacity = 5)
        val bResult = SpaceSaving(capacity = 10).read()
        assertFailsWith<IllegalArgumentException> { a.merge(bResult) }
    }

    @Test
    fun `reset clears state`() {
        val ss = SpaceSaving(capacity = 10)
        for (i in 1..100) ss.update(i.toLong())
        ss.reset()
        val r = ss.read()
        assertEquals(0, r.keys.size)
        assertEquals(0L, r.totalSeen)
    }

    @Test
    fun `create produces independent stat`() {
        val a = SpaceSaving(capacity = 5)
        a.update(1L)
        val b = a.create()
        b.update(2L)
        assertEquals(1, a.read().keys.size)
        assertEquals(1L, a.read().keys[0])
        assertEquals(1, b.read().keys.size)
        assertEquals(2L, b.read().keys[0])
    }

    @Test
    fun `invalid args throw`() {
        assertFailsWith<IllegalArgumentException> { SpaceSaving(capacity = 0) }
        assertFailsWith<IllegalArgumentException> { SpaceSaving(capacity = -1) }
    }
}
