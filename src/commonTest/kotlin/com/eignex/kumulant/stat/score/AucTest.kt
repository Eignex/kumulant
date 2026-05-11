package com.eignex.kumulant.stat.score

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

private const val DELTA = 1e-6

class AucTest {

    @Test
    fun `perfect classifier scores 1`() {
        val auc = AucStat(numBins = 256).apply {
            // Negatives at 0.1, positives at 0.9 — fully separated.
            repeat(50) { update(x = 0.1, y = 0.0) }
            repeat(50) { update(x = 0.9, y = 1.0) }
        }
        assertEquals(1.0, auc.read(0L).auc, DELTA)
    }

    @Test
    fun `inverted classifier scores 0`() {
        val auc = AucStat(numBins = 256).apply {
            repeat(50) { update(x = 0.9, y = 0.0) }
            repeat(50) { update(x = 0.1, y = 1.0) }
        }
        assertEquals(0.0, auc.read(0L).auc, DELTA)
    }

    @Test
    fun `random equal-distribution classifier scores about 0_5`() {
        val auc = AucStat(numBins = 64).apply {
            // Same score distribution for both classes → AUC = 0.5.
            for (i in 0..99) {
                val score = i / 100.0
                update(x = score, y = 0.0)
                update(x = score, y = 1.0)
            }
        }
        assertEquals(0.5, auc.read(0L).auc, 0.01)
    }

    @Test
    fun `single class returns NaN`() {
        val auc = AucStat().apply {
            repeat(10) { update(0.5, 1.0) }
        }
        val r = auc.read(0L)
        assertTrue(r.auc.isNaN())
        assertEquals(10.0, r.totalPositives, DELTA)
        assertEquals(0.0, r.totalNegatives, DELTA)
    }

    @Test
    fun `empty stream returns NaN`() {
        val r = AucStat().read(0L)
        assertTrue(r.auc.isNaN())
        assertEquals(0.0, r.totalPositives, DELTA)
        assertEquals(0.0, r.totalNegatives, DELTA)
    }

    @Test
    fun `out of range scores clamp to edge bins`() {
        val auc = AucStat(numBins = 4, lowerBound = 0.0, upperBound = 1.0).apply {
            update(x = -0.5, y = 0.0) // clamps to bin 0
            update(x = 1.5, y = 1.0) // clamps to bin 3
        }
        // Equivalent to perfectly-separated streams in bin 0 (negatives) vs bin 3 (positives).
        assertEquals(1.0, auc.read(0L).auc, DELTA)
    }

    @Test
    fun `merge two halves yields the same auc as one stream`() {
        fun build(): Pair<AucStat, AucStat> {
            val rng = kotlin.random.Random(42)
            val a = AucStat(numBins = 64)
            val b = AucStat(numBins = 64)
            val ref = AucStat(numBins = 64)
            repeat(100) {
                val score = rng.nextDouble()
                val label = if (rng.nextDouble() < score) 1.0 else 0.0
                if (it < 50) a.update(score, label) else b.update(score, label)
                ref.update(score, label)
            }
            a.merge(b.read(0L))
            return a to ref
        }
        val (merged, ref) = build()
        assertEquals(ref.read(0L).auc, merged.read(0L).auc, DELTA)
    }

    @Test
    fun `weighted updates are honored`() {
        val auc = AucStat(numBins = 64).apply {
            update(0.2, 0.0, weight = 2.0)
            update(0.8, 1.0, weight = 3.0)
        }
        val r = auc.read(0L)
        assertEquals(2.0, r.totalNegatives, DELTA)
        assertEquals(3.0, r.totalPositives, DELTA)
        assertEquals(1.0, r.auc, DELTA)
    }

    @Test
    fun `result equality and hash are content-based`() {
        val a = AucStat(numBins = 8).apply {
            update(x = 0.2, y = 0.0)
            update(x = 0.9, y = 1.0)
        }.read()
        val b = AucStat(numBins = 8).apply {
            update(x = 0.2, y = 0.0)
            update(x = 0.9, y = 1.0)
        }.read()
        val c = AucStat(numBins = 8).apply {
            update(x = 0.2, y = 0.0)
            update(x = 0.5, y = 1.0)
        }.read()

        assertEquals(a, b)
        assertEquals(a.hashCode(), b.hashCode())
        assertNotEquals(a, c)
        assertNotEquals<Any?>(a, "not an AucResult")
    }
}
