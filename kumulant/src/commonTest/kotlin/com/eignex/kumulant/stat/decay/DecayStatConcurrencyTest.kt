package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

/** Cross-mode smoke tests for decay stats — math must match across modes under
 *  sequential single-threaded updates. */
class DecayStatConcurrencyTest {

    private val values = doubleArrayOf(1.0, 2.5, -1.0, 3.0, 0.5, 4.0)
    private val timestamps =
        longArrayOf(0L, 1_000_000_000L, 2_000_000_000L, 3_000_000_000L, 4_000_000_000L, 5_000_000_000L)

    private fun <R : Result> sequentialReads(factory: (Concurrency) -> SeriesStat<R>): Map<Concurrency, R> =
        Concurrency.entries.associateWith { mode ->
            val s = factory(mode)
            for (i in values.indices) s.update(values[i], timestamps[i], 1.0)
            s.read(timestamps.last())
        }

    @Test
    fun `EwmaMeanStat sequential math equal across modes`() {
        val reads = sequentialReads { EwmaMeanStat(alpha = 0.3, concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.totalWeights, r.totalWeights, 1e-9, "EwmaMean totalWeights mode=$mode")
            assertEquals(ref.mean, r.mean, 1e-9, "EwmaMean mean mode=$mode")
        }
    }

    @Test
    fun `EwmaVarianceStat sequential math equal across modes`() {
        val reads = sequentialReads { EwmaVarianceStat(alpha = 0.3, concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.totalWeights, r.totalWeights, 1e-9, "EwmaVariance totalWeights mode=$mode")
            assertEquals(ref.mean, r.mean, 1e-9, "EwmaVariance mean mode=$mode")
            assertEquals(ref.variance, r.variance, 1e-9, "EwmaVariance variance mode=$mode")
        }
    }

    @Test
    fun `DecayingMeanStat sequential math equal across modes`() {
        val reads = sequentialReads { DecayingMeanStat(halfLife = 2.seconds, concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.totalWeights, r.totalWeights, 1e-9, "DecayingMean totalWeights mode=$mode")
            assertEquals(ref.mean, r.mean, 1e-9, "DecayingMean mean mode=$mode")
        }
    }

    @Test
    fun `DecayingVarianceStat sequential math equal across modes`() {
        val reads = sequentialReads { DecayingVarianceStat(halfLife = 2.seconds, concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.totalWeights, r.totalWeights, 1e-9, "DecayingVariance totalWeights mode=$mode")
            assertEquals(ref.mean, r.mean, 1e-9, "DecayingVariance mean mode=$mode")
            assertEquals(ref.variance, r.variance, 1e-9, "DecayingVariance variance mode=$mode")
        }
    }

    @Test
    fun `DecayingSumStat sequential math equal across modes`() {
        val reads = sequentialReads { DecayingSumStat(halfLife = 2.seconds, concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.sum, r.sum, 1e-9, "DecayingSum sum mode=$mode")
        }
    }

    @Test
    fun `HoltStat sequential math equal across modes`() {
        val reads = sequentialReads { HoltStat(alpha = 0.3, beta = 0.1, phi = 0.9, concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.level, r.level, 1e-9, "Holt level mode=$mode")
            assertEquals(ref.trend, r.trend, 1e-9, "Holt trend mode=$mode")
        }
    }

    @Test
    fun `SeasonalSmoothingStat sequential math equal across modes`() {
        val reads = sequentialReads {
            SeasonalSmoothingStat(alpha = 0.3, beta = 0.1, gamma = 0.2, period = 3, concurrency = it)
        }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.level, r.level, 1e-9, "Seasonal level mode=$mode")
            assertEquals(ref.trend, r.trend, 1e-9, "Seasonal trend mode=$mode")
            assertEquals(ref.seasons, r.seasons, "Seasonal seasons mode=$mode")
            assertEquals(ref.currentSlot, r.currentSlot, "Seasonal slot mode=$mode")
        }
    }

    @Test
    fun `RecursiveVarianceStat sequential math equal across modes`() {
        val reads = sequentialReads {
            RecursiveVarianceStat(omega = 0.1, alpha = 0.2, beta = 0.7, concurrency = it)
        }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.variance, r.variance, 1e-9, "RecursiveVariance mode=$mode")
        }
    }
}
