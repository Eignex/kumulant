package com.eignex.kumulant.stat.sketch

import com.eignex.kumulant.stat.sketch.estimate

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class CountMinSketchTest {

    @Test
    fun `empty snapshot is well-formed`() {
        val cms = CountMinSketch(depth = 5, width = 1024)
        val r = cms.read()
        assertEquals(5, r.depth)
        assertEquals(1024, r.width)
        assertEquals(5 * 1024, r.counters.size)
        assertTrue(r.counters.all { it == 0L })
        assertEquals(0L, r.totalSeen)
        assertEquals(0L, r.estimate(42L))
    }

    @Test
    fun `single update is reflected in estimate`() {
        val cms = CountMinSketch(depth = 5, width = 1024)
        cms.update(42L)
        val r = cms.read()
        assertEquals(1L, r.estimate(42L))
        assertEquals(1L, r.totalSeen)
    }

    @Test
    fun `estimate is a one-sided overestimate of true counts`() {
        val cms = CountMinSketch(depth = 5, width = 1024, seed = 17L)
        val truth = mutableMapOf<Long, Long>()
        for (i in 0 until 10_000) {
            val v = (i % 100).toLong()
            cms.update(v)
            truth[v] = (truth[v] ?: 0L) + 1L
        }
        val r = cms.read()
        for ((k, t) in truth) {
            assertTrue(r.estimate(k) >= t, "estimate(${k})=${r.estimate(k)} < true=$t")
        }
    }

    @Test
    fun `mean overestimate is small relative to total seen on uniform stream`() {
        val width = 4096
        val cms = CountMinSketch(depth = 5, width = width, seed = 31L)
        val n = 100_000
        for (i in 0 until n) cms.update((i % 1000).toLong())
        val r = cms.read()
        // True count of each of the 1000 keys is n/1000 = 100; CMS theory gives
        // per-query error ≤ e/w · n with probability ≥ 1 - (1/e)^depth. Average
        // overestimate across queries is comfortably below e/w · n ≈ 67.
        var totalOverestimate = 0L
        for (k in 0L until 1000L) {
            val est = r.estimate(k)
            assertTrue(est >= 100L, "estimate($k)=$est below true count")
            totalOverestimate += (est - 100L)
        }
        val mean = totalOverestimate / 1000.0
        assertTrue(mean < 80.0, "mean overestimate=$mean")
    }

    @Test
    fun `weighted update accumulates`() {
        val cms = CountMinSketch(depth = 5, width = 1024)
        cms.update(7L, weight = 5.0)
        cms.update(7L, weight = 2.0)
        assertEquals(7L, cms.read().estimate(7L))
    }

    @Test
    fun `zero or negative weight is ignored`() {
        val cms = CountMinSketch(depth = 5, width = 1024)
        cms.update(1L, weight = 0.0)
        cms.update(1L, weight = -1.0)
        val r = cms.read()
        assertEquals(0L, r.totalSeen)
        assertEquals(0L, r.estimate(1L))
    }

    @Test
    fun `merge combines two sketches`() {
        val a = CountMinSketch(depth = 5, width = 1024, seed = 9L)
        val b = CountMinSketch(depth = 5, width = 1024, seed = 9L)
        for (i in 0 until 1000) a.update((i % 50).toLong())
        for (i in 0 until 1000) b.update((i % 50).toLong())
        a.merge(b.read())
        val r = a.read()
        assertEquals(2000L, r.totalSeen)
        for (k in 0L until 50L) {
            assertTrue(r.estimate(k) >= 40L, "estimate($k)=${r.estimate(k)}")
        }
    }

    @Test
    fun `merge requires matching shape`() {
        val a = CountMinSketch(depth = 5, width = 1024, seed = 1L)
        val bResult = CountMinSketch(depth = 5, width = 1024, seed = 2L).read()
        assertFailsWith<IllegalArgumentException> { a.merge(bResult) }

        val cResult = CountMinSketch(depth = 4, width = 1024, seed = 1L).read()
        assertFailsWith<IllegalArgumentException> { a.merge(cResult) }

        val dResult = CountMinSketch(depth = 5, width = 512, seed = 1L).read()
        assertFailsWith<IllegalArgumentException> { a.merge(dResult) }
    }

    @Test
    fun `reset clears state`() {
        val cms = CountMinSketch(depth = 5, width = 1024)
        for (i in 1..100) cms.update(i.toLong())
        cms.reset()
        val r = cms.read()
        assertEquals(0L, r.totalSeen)
        assertTrue(r.counters.all { it == 0L })
    }

    @Test
    fun `create produces independent stat with same shape`() {
        val a = CountMinSketch(depth = 5, width = 1024, seed = 99L)
        a.update(7L)
        val b = a.create()
        b.update(8L)
        assertEquals(1L, a.read().estimate(7L))
        assertEquals(0L, a.read().estimate(8L))
        assertEquals(1L, b.read().estimate(8L))
    }

    @Test
    fun `invalid args throw`() {
        assertFailsWith<IllegalArgumentException> { CountMinSketch(depth = 0, width = 1024) }
        assertFailsWith<IllegalArgumentException> { CountMinSketch(depth = 5, width = 0) }
        assertFailsWith<IllegalArgumentException> { CountMinSketch(depth = 5, width = 1000) }
    }
}
