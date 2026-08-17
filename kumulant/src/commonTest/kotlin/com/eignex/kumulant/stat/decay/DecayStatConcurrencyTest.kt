package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.timedReads
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

/** Cross-mode smoke tests for decay stats; math must match across modes under
 *  sequential single-threaded updates. */
class DecayStatConcurrencyTest {

    @Test
    fun `EwmaMeanStat sequential math equal across modes`() {
        val reads = timedReads { EwmaMeanStat(alpha = 0.3, concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.totalWeights, r.totalWeights, 1e-9, "EwmaMean totalWeights mode=$mode")
            assertEquals(ref.mean, r.mean, 1e-9, "EwmaMean mean mode=$mode")
        }
    }

    @Test
    fun `EwmaVarianceStat sequential math equal across modes`() {
        val reads = timedReads { EwmaVarianceStat(alpha = 0.3, concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.totalWeights, r.totalWeights, 1e-9, "EwmaVariance totalWeights mode=$mode")
            assertEquals(ref.mean, r.mean, 1e-9, "EwmaVariance mean mode=$mode")
            assertEquals(ref.variance, r.variance, 1e-9, "EwmaVariance variance mode=$mode")
        }
    }

    @Test
    fun `DecayingMeanStat sequential math equal across modes`() {
        val reads = timedReads { DecayingMeanStat(halfLife = 2.seconds, concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.totalWeights, r.totalWeights, 1e-9, "DecayingMean totalWeights mode=$mode")
            assertEquals(ref.mean, r.mean, 1e-9, "DecayingMean mean mode=$mode")
        }
    }

    @Test
    fun `DecayingVarianceStat sequential math equal across modes`() {
        val reads = timedReads { DecayingVarianceStat(halfLife = 2.seconds, concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.totalWeights, r.totalWeights, 1e-9, "DecayingVariance totalWeights mode=$mode")
            assertEquals(ref.mean, r.mean, 1e-9, "DecayingVariance mean mode=$mode")
            assertEquals(ref.variance, r.variance, 1e-9, "DecayingVariance variance mode=$mode")
        }
    }

    @Test
    fun `DecayingSumStat sequential math equal across modes`() {
        val reads = timedReads { DecayingSumStat(halfLife = 2.seconds, concurrency = it) }
        val ref = reads.getValue(Concurrency.None)
        for ((mode, r) in reads) {
            assertEquals(ref.sum, r.sum, 1e-9, "DecayingSum sum mode=$mode")
        }
    }
}
