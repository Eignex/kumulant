package com.eignex.kumulant.operation

import com.eignex.kumulant.stat.summary.Sum

import kotlin.test.Test
import kotlin.test.assertEquals

private const val DELTA = 1e-12

class FoldsTest {

    @Test
    fun `foldPaired computes difference y-x`() {
        val stat = Sum().foldPaired { x, y -> y - x }
        stat.update(3.0, 10.0)
        stat.update(1.0, 4.0)
        assertEquals(10.0, stat.read().sum, DELTA)
    }

    @Test
    fun `foldVector computes sum of elements`() {
        val stat = Sum().foldVector { v -> v.sum() }
        stat.update(doubleArrayOf(1.0, 2.0, 3.0))
        assertEquals(6.0, stat.read().sum, DELTA)
    }
}
