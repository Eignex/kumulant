package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Cross-mode smoke tests: each summary stat should produce identical math under
 * sequential single-threaded updates regardless of the [Concurrency] mode chosen.
 * Catches mode-branch wiring bugs (wrong cell type, missing lock, etc.) cheaply
 * before the bench-side concurrency tests exercise actual contention.
 */
class SummaryStatConcurrencyTest {

    private val values = doubleArrayOf(1.0, -2.0, 3.5, 0.0, 4.2, -1.1, 7.0, 2.5)
    private val weights = doubleArrayOf(1.0, 2.0, 1.0, 3.0, 1.0, 1.0, 2.5, 0.5)

    private fun <R : Result> sequentialReads(factory: (Concurrency) -> SeriesStat<R>): Map<Concurrency, R> =
        Concurrency.entries.associateWith { mode ->
            val s = factory(mode)
            for (i in values.indices) s.update(values[i], 0L, weights[i])
            s.read(0L)
        }

    @Test
    fun `SumStat sequential math equal across modes`() {
        val reads = sequentialReads { SumStat(concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) assertEquals(ref, r, "SumStat mode=$mode")
    }

    @Test
    fun `CountStat sequential math equal across modes`() {
        val reads = sequentialReads { CountStat(concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) assertEquals(ref, r, "CountStat mode=$mode")
    }

    @Test
    fun `TotalWeightsStat sequential math equal across modes`() {
        val reads = sequentialReads { TotalWeightsStat(concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) assertEquals(ref, r, "TotalWeightsStat mode=$mode")
    }

    @Test
    fun `MeanStat sequential math equal across modes`() {
        val reads = sequentialReads { MeanStat(concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.totalWeights, r.totalWeights, 1e-9, "MeanStat totalWeights mode=$mode")
            assertEquals(ref.mean, r.mean, 1e-9, "MeanStat mean mode=$mode")
        }
    }

    @Test
    fun `VarianceStat sequential math equal across modes`() {
        val reads = sequentialReads { VarianceStat(concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.totalWeights, r.totalWeights, 1e-9, "VarianceStat totalWeights mode=$mode")
            assertEquals(ref.mean, r.mean, 1e-9, "VarianceStat mean mode=$mode")
            assertEquals(ref.variance, r.variance, 1e-9, "VarianceStat variance mode=$mode")
        }
    }

    @Test
    fun `MomentsStat sequential math equal across modes`() {
        val reads = sequentialReads { MomentsStat(concurrency = it) }
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
        val reads = sequentialReads { MinStat(concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) assertEquals(ref, r, "MinStat mode=$mode")
    }

    @Test
    fun `MaxStat sequential math equal across modes`() {
        val reads = sequentialReads { MaxStat(concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) assertEquals(ref, r, "MaxStat mode=$mode")
    }

    @Test
    fun `RangeStat sequential math equal across modes`() {
        val reads = sequentialReads { RangeStat(concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) assertEquals(ref, r, "RangeStat mode=$mode")
    }

    @Test
    fun `BernoulliSumStat sequential math equal across modes`() {
        val bernValues = doubleArrayOf(1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 0.0, 0.0)
        val reads = Concurrency.entries.associateWith { mode ->
            val s = BernoulliSumStat(concurrency = mode)
            for (i in bernValues.indices) s.update(bernValues[i], 0L, weights[i])
            s.read(0L)
        }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) assertEquals(ref, r, "BernoulliSumStat mode=$mode")
    }

    @Test
    fun `merge across modes preserves math`() {
        val a = SumStat(concurrency = Concurrency.None).apply { for (v in values) update(v, 0L, 1.0) }
        for (mode in Concurrency.entries) {
            val b = SumStat(concurrency = mode).apply { for (v in values) update(v, 0L, 1.0) }
            b.merge(a.read(0L))
            // After merge: b has 2x of values' sum.
            assertEquals(2 * values.sum(), b.read(0L).sum, 1e-9, "merge mode=$mode")
        }
    }

    @Test
    fun `reset returns to initial across modes`() {
        for (mode in Concurrency.entries) {
            val s = MeanStat(concurrency = mode)
            for (v in values) s.update(v, 0L)
            s.reset()
            val r = s.read(0L)
            assertEquals(0.0, r.totalWeights, "reset mode=$mode")
            assertEquals(0.0, r.mean, "reset mode=$mode")
        }
    }
}
