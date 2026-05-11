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
    fun `series withValue replaces input with constant`() {
        val stat = SumStat().withValue(7.0)
        stat.update(1.0)
        stat.update(2.0)
        assertEquals(14.0, stat.read().sum, DELTA)
    }

    @Test
    fun `discrete withValue replaces input with constant`() {
        val stat = LinearCountingStat(bits = 1024).withValue(7L)
        for (i in 1L..100L) stat.update(i)
        val seen = stat.read().estimate
        assertTrue(seen in 0.5..2.0, "estimate=$seen")
    }
}

class BridgesTest {

    @Test
    fun `discrete asSeries casts Double to Long via truncation`() {
        val stat = HyperLogLogStat(precision = 10).asSeries()
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
