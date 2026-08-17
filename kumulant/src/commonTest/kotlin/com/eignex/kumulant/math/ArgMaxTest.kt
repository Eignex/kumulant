package com.eignex.kumulant.math

import kotlin.test.Test
import kotlin.test.assertEquals

// The two conventions interact: seeding the running best at `-Infinity` is what makes a NaN score
// lose, and strict `>` is what resolves a tie to the lowest index.
class ArgMaxTest {

    @Test
    fun `the largest score wins`() {
        assertEquals(2, argMaxOf(4) { doubleArrayOf(1.0, 3.0, 9.0, 2.0)[it] })
    }

    @Test
    fun `a tie resolves to the lowest index`() {
        // Relied on by every caller: ClassCounts documents it, and a forest's per-class vote is
        // routinely tied on a leaf that saw one observation of each class.
        assertEquals(1, argMaxOf(4) { doubleArrayOf(0.0, 5.0, 5.0, 5.0)[it] })
    }

    @Test
    fun `a NaN score loses rather than winning`() {
        // Seeding from `score(0)` would make a NaN first score unbeatable, because every comparison
        // against NaN is false, so one poisoned class would decide every later prediction.
        assertEquals(1, argMaxOf(3) { doubleArrayOf(Double.NaN, 2.0, 1.0)[it] })
        assertEquals(0, argMaxOf(3) { doubleArrayOf(2.0, Double.NaN, 1.0)[it] })
    }

    @Test
    fun `an all-NaN set falls through to the first index`() {
        // There is no meaningful answer here, so the requirement is only that it is defined and does
        // not throw - which is the whole of the non-finite guarantee.
        assertEquals(0, argMaxOf(3) { Double.NaN })
    }

    @Test
    fun `negative infinity is still beaten by a real score`() {
        // The seed value is -Infinity, so an arm genuinely scoring -Infinity must not be mistaken for
        // "nothing seen yet" in a way that changes the winner.
        assertEquals(2, argMaxOf(3) { doubleArrayOf(Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, -5.0)[it] })
    }

    @Test
    fun `a single candidate is its own argmax`() {
        assertEquals(0, argMaxOf(1) { -17.0 })
    }
}
