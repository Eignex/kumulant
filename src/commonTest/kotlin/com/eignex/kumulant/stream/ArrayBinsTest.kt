package com.eignex.kumulant.stream

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private const val DELTA = 1e-12

class ArrayBinsTest {

    @Test
    fun `snapshot is empty before any add`() {
        val bins = ArrayBins(SerialMode)
        assertTrue(bins.snapshot().isEmpty())
    }

    @Test
    fun `single add produces single entry in snapshot`() {
        val bins = ArrayBins(SerialMode)
        bins.add(0, 1.5)
        val snap = bins.snapshot()
        assertEquals(mapOf(0 to 1.5), snap)
    }

    @Test
    fun `multiple adds to the same index accumulate weight`() {
        val bins = ArrayBins(SerialMode)
        bins.add(5, 1.0)
        bins.add(5, 2.0)
        bins.add(5, 0.5)
        assertEquals(3.5, bins.snapshot()[5]!!, DELTA)
    }

    @Test
    fun `supports negative indices`() {
        val bins = ArrayBins(SerialMode)
        bins.add(-10, 2.0)
        bins.add(-5, 3.0)
        val snap = bins.snapshot()
        assertEquals(2.0, snap[-10]!!, DELTA)
        assertEquals(3.0, snap[-5]!!, DELTA)
    }

    @Test
    fun `grows to accommodate index below initial offset`() {
        val bins = ArrayBins(SerialMode)
        bins.add(0, 1.0)
        bins.add(-200, 2.0)
        val snap = bins.snapshot()
        assertEquals(1.0, snap[0]!!, DELTA)
        assertEquals(2.0, snap[-200]!!, DELTA)
    }

    @Test
    fun `grows to accommodate index above initial offset plus length`() {
        val bins = ArrayBins(SerialMode)
        bins.add(0, 1.0)
        bins.add(10_000, 2.0)
        val snap = bins.snapshot()
        assertEquals(1.0, snap[0]!!, DELTA)
        assertEquals(2.0, snap[10_000]!!, DELTA)
    }

    @Test
    fun `existing StreamDouble instances are preserved across resize`() {
        val bins = ArrayBins(SerialMode)
        bins.add(0, 1.0)
        bins.add(1, 2.0)

        bins.add(10_000, 3.0)

        bins.add(0, 4.0)
        bins.add(1, 5.0)
        val snap = bins.snapshot()
        assertEquals(5.0, snap[0]!!, DELTA)
        assertEquals(7.0, snap[1]!!, DELTA)
        assertEquals(3.0, snap[10_000]!!, DELTA)
    }

    @Test
    fun `snapshot excludes zero-weight entries`() {
        val bins = ArrayBins(SerialMode)
        bins.add(0, 1.0)

        val snap = bins.snapshot()
        assertEquals(setOf(0), snap.keys)
    }

    @Test
    fun `clear empties the snapshot`() {
        val bins = ArrayBins(SerialMode)
        bins.add(0, 1.0)
        bins.add(50, 2.0)
        bins.clear()
        assertTrue(bins.snapshot().isEmpty())
    }

    @Test
    fun `clear followed by add starts from scratch`() {
        val bins = ArrayBins(SerialMode)
        bins.add(0, 1.0)
        bins.clear()
        bins.add(7, 2.0)
        assertEquals(mapOf(7 to 2.0), bins.snapshot())
    }

    @Test
    fun `repeated resizes accumulate correctly`() {
        val bins = ArrayBins(SerialMode)

        bins.add(0, 1.0)
        bins.add(500, 1.0)
        bins.add(5_000, 1.0)
        bins.add(50_000, 1.0)
        val snap = bins.snapshot()
        assertEquals(4, snap.size)
        assertEquals(1.0, snap[0]!!, DELTA)
        assertEquals(1.0, snap[500]!!, DELTA)
        assertEquals(1.0, snap[5_000]!!, DELTA)
        assertEquals(1.0, snap[50_000]!!, DELTA)
    }
}
