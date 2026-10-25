package com.eignex.kumulant.math

import kotlin.test.Test
import kotlin.test.assertEquals

class ArgMaxTest {

    @Test
    fun `the largest score wins`() {
        assertEquals(2, argMaxOf(4) { doubleArrayOf(1.0, 3.0, 9.0, 2.0)[it] })
    }

    @Test
    fun `a tie resolves to the lowest index`() {
        assertEquals(1, argMaxOf(4) { doubleArrayOf(0.0, 5.0, 5.0, 5.0)[it] })
    }

    @Test
    fun `a NaN score loses rather than winning`() {
        assertEquals(1, argMaxOf(3) { doubleArrayOf(Double.NaN, 2.0, 1.0)[it] })
        assertEquals(0, argMaxOf(3) { doubleArrayOf(2.0, Double.NaN, 1.0)[it] })
    }

    @Test
    fun `an all-NaN set falls through to the first index`() {
        assertEquals(0, argMaxOf(3) { Double.NaN })
    }

    @Test
    fun `negative infinity is still beaten by a real score`() {
        assertEquals(2, argMaxOf(3) { doubleArrayOf(Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, -5.0)[it] })
    }

    @Test
    fun `a single candidate is its own argmax`() {
        assertEquals(0, argMaxOf(1) { -17.0 })
    }
}
