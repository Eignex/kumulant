package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.core.Concurrency
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class QuantileStatConcurrencyTest {

    private val values = doubleArrayOf(0.1, 1.2, 3.7, 0.5, 2.8, 1.0, 4.5, 0.3, 2.1, 1.7, 3.0, 0.8)

    @Test
    fun `LinearHistogramStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = LinearHistogramStat(
                lowerBound = 0.0,
                upperBound = 5.0,
                binCount = 10,
                concurrency = mode,
            )
            for (v in values) s.update(v)
            s.read(0L)
        }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertTrue(ref.weights.contentEquals(r.weights), "LinearHistogram weights mode=$mode")
        }
    }

    @Test
    fun `DDSketchStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = DDSketchStat(relativeError = 0.05, concurrency = mode)
            for (v in values) s.update(v)
            s.read(0L)
        }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            for (i in ref.probabilities.indices) {
                assertEquals(ref.quantiles[i], r.quantiles[i], 1e-9, "DDSketch q=${ref.probabilities[i]} mode=$mode")
            }
        }
    }

    @Test
    fun `TDigestStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = TDigestStat(compression = 100.0, concurrency = mode)
            for (v in values) s.update(v)
            s.read(0L)
        }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            for (i in ref.probabilities.indices) {
                assertEquals(ref.quantiles[i], r.quantiles[i], 1e-9, "TDigest q=${ref.probabilities[i]} mode=$mode")
            }
        }
    }

    @Test
    fun `HdrHistogramStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = HdrHistogramStat(concurrency = mode)
            for (v in values) s.update(v)
            s.read(0L)
        }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertTrue(ref.weights.contentEquals(r.weights), "HdrHistogram weights mode=$mode")
        }
    }

    @Test
    fun `FrugalQuantileStat sequential math equal across modes`() {
        val reads = Concurrency.entries.associateWith { mode ->
            val s = FrugalQuantileStat(q = 0.5, concurrency = mode)
            for (v in values) s.update(v)
            s.read(0L)
        }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.quantile, r.quantile, 1e-9, "FrugalQuantile mode=$mode")
        }
    }
}
