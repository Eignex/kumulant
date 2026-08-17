package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.WEIGHTED_VALUES
import com.eignex.kumulant.WEIGHTED_WEIGHTS
import com.eignex.kumulant.assertModesAgree
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.weightedReads
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Cross-mode smoke tests: each summary stat should produce identical math under
 * sequential single-threaded updates regardless of the [Concurrency] mode chosen.
 * Catches mode-branch wiring bugs (wrong cell type, missing lock, etc.) cheaply
 * before the bench-side concurrency tests exercise actual contention.
 */
class SummaryStatConcurrencyTest {

    @Test
    fun `SumStat sequential math equal across modes`() {
        val reads = weightedReads { SumStat(concurrency = it) }
        assertModesAgree("SumStat", reads)
    }

    @Test
    fun `CountStat sequential math equal across modes`() {
        val reads = weightedReads { CountStat(concurrency = it) }
        assertModesAgree("CountStat", reads)
    }

    @Test
    fun `TotalWeightsStat sequential math equal across modes`() {
        val reads = weightedReads { TotalWeightsStat(concurrency = it) }
        assertModesAgree("TotalWeightsStat", reads)
    }

    @Test
    fun `ArgMinStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = ArgMinStat(concurrency = mode)
            for (i in WEIGHTED_VALUES.indices) s.update(WEIGHTED_VALUES[i], i.toLong(), WEIGHTED_WEIGHTS[i])
            s.read(0L)
        }
        assertModesAgree("ArgMinStat", reads)
    }

    @Test
    fun `ArgMaxStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = ArgMaxStat(concurrency = mode)
            for (i in WEIGHTED_VALUES.indices) s.update(WEIGHTED_VALUES[i], i.toLong(), WEIGHTED_WEIGHTS[i])
            s.read(0L)
        }
        assertModesAgree("ArgMaxStat", reads)
    }

    @Test
    fun `MeanStat sequential math equal across modes`() {
        val reads = weightedReads { MeanStat(concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.totalWeights, r.totalWeights, 1e-9, "MeanStat totalWeights mode=$mode")
            assertEquals(ref.mean, r.mean, 1e-9, "MeanStat mean mode=$mode")
        }
    }

    @Test
    fun `VarianceStat sequential math equal across modes`() {
        val reads = weightedReads { VarianceStat(concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.totalWeights, r.totalWeights, 1e-9, "VarianceStat totalWeights mode=$mode")
            assertEquals(ref.mean, r.mean, 1e-9, "VarianceStat mean mode=$mode")
            assertEquals(ref.variance, r.variance, 1e-9, "VarianceStat variance mode=$mode")
        }
    }

    @Test
    fun `MomentsStat sequential math equal across modes`() {
        val reads = weightedReads { MomentsStat(concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.totalWeights, r.totalWeights, 1e-9, "MomentsStat totalWeights mode=$mode")
            assertEquals(ref.mean, r.mean, 1e-9, "MomentsStat mean mode=$mode")
            assertEquals(ref.m2, r.m2, 1e-9, "MomentsStat m2 mode=$mode")
            assertEquals(ref.m3, r.m3, 1e-9, "MomentsStat m3 mode=$mode")
            assertEquals(ref.m4, r.m4, 1e-9, "MomentsStat m4 mode=$mode")
        }
    }

    @Test
    fun `MinStat sequential math equal across modes`() {
        val reads = weightedReads { MinStat(concurrency = it) }
        assertModesAgree("MinStat", reads)
    }

    @Test
    fun `MaxStat sequential math equal across modes`() {
        val reads = weightedReads { MaxStat(concurrency = it) }
        assertModesAgree("MaxStat", reads)
    }

    @Test
    fun `RangeStat sequential math equal across modes`() {
        val reads = weightedReads { RangeStat(concurrency = it) }
        assertModesAgree("RangeStat", reads)
    }

    @Test
    fun `BernoulliSumStat sequential math equal across modes`() {
        val bernValues = doubleArrayOf(1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 0.0, 0.0)
        val reads = Concurrency.entries.associateWith { mode ->
            val s = BernoulliSumStat(concurrency = mode)
            for (i in bernValues.indices) s.update(bernValues[i], 0L, WEIGHTED_WEIGHTS[i])
            s.read(0L)
        }
        assertModesAgree("BernoulliSumStat", reads)
    }

    @Test
    fun `merge across modes preserves math`() {
        val a = SumStat(concurrency = Concurrency.None).apply { for (v in WEIGHTED_VALUES) update(v, 0L, 1.0) }
        for (mode in Concurrency.entries) {
            val b = SumStat(concurrency = mode).apply { for (v in WEIGHTED_VALUES) update(v, 0L, 1.0) }
            b.merge(a.read(0L))
            // After merge: b has 2x of WEIGHTED_VALUES' sum.
            assertEquals(2 * WEIGHTED_VALUES.sum(), b.read(0L).sum, 1e-9, "merge mode=$mode")
        }
    }

    @Test
    fun `reset returns to initial across modes`() {
        for (mode in Concurrency.entries) {
            val s = MeanStat(concurrency = mode)
            for (v in WEIGHTED_VALUES) s.update(v, 0L)
            s.reset()
            val r = s.read(0L)
            assertEquals(0.0, r.totalWeights, "reset mode=$mode")
            assertEquals(0.0, r.mean, "reset mode=$mode")
        }
    }
}
