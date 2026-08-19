package com.eignex.kumulant.stat.regression.tree

import com.eignex.kumulant.DELTA
import kotlin.test.Test
import kotlin.test.assertEquals

class ClassCountsResultTest {

    @Test
    fun `probabilities normalise to one`() {
        val r = ClassCountsResult(numClasses = 3, counts = doubleArrayOf(1.0, 2.0, 5.0))
        val p = r.probabilities()
        assertEquals(1.0, p.sum(), DELTA)
        assertEquals(5.0 / 8.0, p[2], DELTA)
    }

    @Test
    fun `predict returns the majority class`() {
        val r = ClassCountsResult(numClasses = 4, counts = doubleArrayOf(1.0, 4.0, 2.0, 3.0))
        assertEquals(1, r.predict())
    }

    @Test
    fun `gini and entropy are zero on a pure leaf`() {
        val pure = ClassCountsResult(3, doubleArrayOf(0.0, 10.0, 0.0))
        assertEquals(0.0, pure.gini, DELTA)
        assertEquals(0.0, pure.entropy, DELTA)
    }

    @Test
    fun `empty leaf falls back to uniform probabilities`() {
        val empty = ClassCountsResult(3, doubleArrayOf(0.0, 0.0, 0.0))
        val p = empty.probabilities()
        for (k in 0 until 3) assertEquals(1.0 / 3.0, p[k], DELTA)
    }

    @Test
    fun `ClassCountsStat increments counts on update`() {
        val s = ClassCountsStat(3)
        s.update(0.0)
        s.update(2.0, weight = 3.0)
        val r = s.read()
        assertEquals(1.0, r.counts[0], DELTA)
        assertEquals(0.0, r.counts[1], DELTA)
        assertEquals(3.0, r.counts[2], DELTA)
    }

    @Test
    fun `subtractCC inverts mergeCC when a class count is negative`() {
        val a = ClassCountsResult(2, doubleArrayOf(6.0, -5.0))
        val b = ClassCountsResult(2, doubleArrayOf(4.0, 2.0))
        assertEquals(a, subtractCC(mergeCC(a, b), b))
    }
}
