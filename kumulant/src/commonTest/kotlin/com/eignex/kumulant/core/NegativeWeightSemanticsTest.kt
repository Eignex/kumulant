package com.eignex.kumulant.core

import com.eignex.kumulant.stat.decay.DecayingMeanStat
import com.eignex.kumulant.stat.decay.DecayingSumStat
import com.eignex.kumulant.stat.decay.EwmaMeanStat
import com.eignex.kumulant.stat.quantile.ThresholdBucketStat
import com.eignex.kumulant.stat.summary.MeanStat
import com.eignex.kumulant.stat.summary.SumStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

// The contract: zero is a no-op, and a negative weight does whatever is standard for the specific
// statistic, throwing only where the alternative is a corrupted accumulator.
class NegativeWeightSemanticsTest {

    @Test
    fun `additive stats subtract`() {
        val sum = SumStat()
        listOf(10.0, 20.0, 30.0).forEach { sum.update(it) }
        sum.update(5.0, weight = -1.0)
        assertEquals(55.0, sum.read().sum, 1e-9)
    }

    @Test
    fun `a decaying sum subtracts and round-trips`() {
        val decaying = DecayingSumStat(30.seconds)
        listOf(10.0, 20.0).forEach { decaying.update(it, 0L, 1.0) }
        val before = decaying.read(0L).sum
        decaying.update(7.0, 0L, 1.0)
        decaying.update(7.0, 0L, -1.0)
        assertEquals(before, decaying.read(0L).sum, 1e-9)
    }

    @Test
    fun `an EWMA downdate restores the prior state`() {
        val ewma = EwmaMeanStat(alpha = 0.3)
        listOf(10.0, 20.0, 30.0).forEach { ewma.update(it, 0L, 1.0) }
        val before = ewma.read(0L)

        repeat(6) { ewma.update(5.0, 0L, -1.0) }
        // Over-subtracting drives the accumulated weight negative. The reported mean goes with
        // it rather than becoming non-finite, and nothing is wedged.
        val over = ewma.read(0L)
        assertTrue(over.totalWeights < 0.0, "expected a negative accumulated weight, got $over")
        assertTrue(over.mean.isFinite(), "mean should stay finite, was ${over.mean}")

        repeat(6) { ewma.update(5.0, 0L, 1.0) }
        val after = ewma.read(0L)
        assertEquals(before.totalWeights, after.totalWeights, 1e-9)
        assertEquals(before.mean, after.mean, 1e-9)
    }

    @Test
    fun `a decaying mean reports NaN on negative effective weight and recovers`() {
        val decaying = DecayingMeanStat(30.seconds)
        listOf(10.0, 20.0, 30.0).forEach { decaying.update(it, 0L, 1.0) }
        repeat(6) { decaying.update(5.0, 0L, -1.0) }

        // This NaN is deliberate, not corruption: `read` has an explicit branch for a negative
        // decayed weight, because there is no meaningful mean of a negative amount of evidence.
        // It is a sentinel rather than a wedged state, which the recovery below establishes.
        val over = decaying.read(0L)
        assertTrue(over.totalWeights < 0.0, "expected a negative decayed weight, got $over")
        assertTrue(over.mean.isNaN(), "expected the NaN sentinel, got ${over.mean}")

        repeat(6) { decaying.update(5.0, 0L, 1.0) }
        val after = decaying.read(0L)
        assertTrue(after.mean.isFinite(), "mean should recover once weight is positive again")
        assertEquals(20.0, after.mean, 1e-6)
    }

    @Test
    fun `a bucket count can go negative so callers must not read it as a population`() {
        val buckets = ThresholdBucketStat(doubleArrayOf(5.0, 15.0, 25.0))
        buckets.update(1.0, weight = -1.0)
        // Documented consequence of subtraction on a counting stat: the bucket is a signed
        // accumulation, not a population count, so it can go below zero. Guarding it would
        // break the legitimate case of retracting an observation that was counted earlier.
        assertEquals(-1.0, buckets.read().counts[0], 1e-9)
        buckets.update(1.0, weight = 1.0)
        assertEquals(0.0, buckets.read().counts[0], 1e-9)
    }

    @Test
    fun `Welford stats reject only the downdate that cannot be recovered`() {
        val mean = MeanStat()
        mean.update(10.0, weight = 2.0)
        // A downdate inside the accumulated weight is fine.
        mean.update(10.0, weight = -1.0)
        assertEquals(1.0, mean.read().totalWeights, 1e-9)
        // Exhausting it is not: every Welford step divides by the new total, so a zero or
        // negative total leaves a permanently non-finite accumulator.
        assertFailsWith<IllegalArgumentException> { mean.update(10.0, weight = -1.0) }
        assertTrue(mean.read().mean.isFinite())
    }
}
