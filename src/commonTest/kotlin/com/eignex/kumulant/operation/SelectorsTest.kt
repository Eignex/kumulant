package com.eignex.kumulant.operation

import com.eignex.kumulant.stat.Sum
import kotlin.test.Test
import kotlin.test.assertEquals

private const val DELTA = 1e-12

class SelectorsTest {

    @Test
    fun `atX feeds x to underlying stat`() {
        val stat = Sum().atX()
        stat.update(10.0, 99.0)
        assertEquals(10.0, stat.read().sum, DELTA)
    }

    @Test
    fun `atY feeds y to underlying stat`() {
        val stat = Sum().atY()
        stat.update(99.0, 7.0)
        assertEquals(7.0, stat.read().sum, DELTA)
    }

    @Test
    fun `atIndex picks single element from vector`() {
        val stat = Sum().atIndex(1)
        stat.update(doubleArrayOf(10.0, 20.0, 30.0))
        assertEquals(20.0, stat.read().sum, DELTA)
    }

    @Test
    fun `atIndices extracts two elements for paired stat`() {
        val stat = Sum().atX().atIndices(0, 2)
        stat.update(doubleArrayOf(5.0, 99.0, 3.0))
        assertEquals(5.0, stat.read().sum, DELTA)
    }
}
