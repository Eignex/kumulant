package com.eignex.kumulant.operation

import com.eignex.kumulant.stat.cardinality.HyperLogLogStat
import com.eignex.kumulant.stat.cardinality.LinearCountingStat
import com.eignex.kumulant.stat.summary.SumStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private const val DELTA = 1e-12

class TransformsTest {

    @Test
    fun `series transformValue is applied before update`() {
        val stat = SumStat().transformValue { it * 2.0 }
        stat.update(3.0)
        stat.update(4.0)
        assertEquals(14.0, stat.read().sum, DELTA)
    }

    @Test
    fun `series withValue replaces input with constant`() {
        val stat = SumStat().withValue(7.0)
        stat.update(1.0)
        stat.update(2.0)
        assertEquals(14.0, stat.read().sum, DELTA)
    }

    @Test
    fun `paired transformPair maps both axes`() {
        val stat = SumStat().atX().transformPair { x, y -> (x + 1.0) to (y + 1.0) }
        stat.update(2.0, 3.0)
        stat.update(4.0, 5.0)
        assertEquals(8.0, stat.read().sum, DELTA) // (2+1) + (4+1)
    }

    @Test
    fun `paired transformX rewrites only x`() {
        val stat = SumStat().atX().transformX { it * 10.0 }
        stat.update(2.0, 99.0)
        stat.update(3.0, 99.0)
        assertEquals(50.0, stat.read().sum, DELTA)
    }

    @Test
    fun `paired transformY rewrites only y`() {
        val stat = SumStat().atY().transformY { it * 10.0 }
        stat.update(99.0, 2.0)
        stat.update(99.0, 3.0)
        assertEquals(50.0, stat.read().sum, DELTA)
    }

    @Test
    fun `discrete transformValue collapses inputs into buckets`() {
        val stat = HyperLogLogStat(precision = 10).transformValue { it / 10L }
        for (i in 0L..99L) stat.update(i)
        val seen = stat.read().estimate
        assertTrue(seen in 8.0..12.0, "estimate=$seen")
    }

    @Test
    fun `discrete withValue replaces input with constant`() {
        val stat = LinearCountingStat(bits = 1024).withValue(7L)
        for (i in 1L..100L) stat.update(i)
        val seen = stat.read().estimate
        assertTrue(seen in 0.5..2.0, "estimate=$seen")
    }

    @Test
    fun `DoubleTransform fun-interface lambda compiles and runs`() {
        val t = DoubleTransform { it + 1.0 }
        assertEquals(4.0, t.apply(3.0), DELTA)
    }

    @Test
    fun `LongTransform fun-interface lambda compiles and runs`() {
        val t = LongTransform { it + 1L }
        assertEquals(4L, t.apply(3L))
    }
}

class BridgesTest {

    @Test
    fun `discrete asSeries casts Double to Long via truncation`() {
        val stat = HyperLogLogStat(precision = 10).asSeries()
        // 1.5, 2.7, 2.9 truncate to 1L, 2L, 2L → 2 distinct keys
        stat.update(1.5)
        stat.update(2.7)
        stat.update(2.9)
        val seen = stat.read().estimate
        assertTrue(seen in 1.5..2.5, "estimate=$seen")
    }

    @Test
    fun `series asDiscrete casts Long to Double`() {
        val stat = SumStat().asDiscrete()
        stat.update(1L)
        stat.update(2L)
        stat.update(3L)
        assertEquals(6.0, stat.read().sum, DELTA)
    }

    @Test
    fun `discrete asSeries composes with atY for paired streams`() {
        val pairedHll = HyperLogLogStat(precision = 10).asSeries().atY()
        for (i in 1L..50L) pairedHll.update(0.0, i.toDouble())
        // 50 distinct y values regardless of x
        assertTrue(pairedHll.read().estimate > 30.0)
    }

    @Test
    fun `discrete asSeries create produces independent stat`() {
        val template = HyperLogLogStat(precision = 10).asSeries()
        val fresh = template.create()
        for (i in 1..100) fresh.update(i.toDouble())
        assertEquals(0.0, template.read().estimate)
        assertTrue(fresh.read().estimate > 50.0)
    }

    @Test
    fun `series asDiscrete create produces independent stat`() {
        val template = SumStat().asDiscrete()
        val fresh = template.create()
        fresh.update(5L)
        assertEquals(0.0, template.read().sum, DELTA)
        assertEquals(5.0, fresh.read().sum, DELTA)
    }
}

class TransformLifecycleTest {

    @Test
    fun `transformValue abs value`() {
        val stat = SumStat().transformValue { if (it < 0) -it else it }
        stat.update(-4.0)
        stat.update(3.0)
        assertEquals(7.0, stat.read().sum, DELTA)
    }

    @Test
    fun `create of transformValue stat is independent`() {
        val s1 = SumStat().transformValue { it * 10.0 }
        s1.update(1.0)
        val s2 = s1.create()
        s2.update(1.0)
        assertEquals(10.0, s1.read().sum, DELTA)
        assertEquals(10.0, s2.read().sum, DELTA)
    }

    @Test
    fun `reset on transformValue clears underlying stat`() {
        val stat = SumStat().transformValue { it * 2.0 }
        stat.update(5.0)
        stat.reset()
        assertEquals(0.0, stat.read().sum, DELTA)
    }
}
