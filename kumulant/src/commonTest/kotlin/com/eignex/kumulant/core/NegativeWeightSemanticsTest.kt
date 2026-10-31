package com.eignex.kumulant.core

import com.eignex.kumulant.stat.decay.DecayingMeanStat
import com.eignex.kumulant.stat.decay.DecayingSumStat
import com.eignex.kumulant.stat.decay.EwmaMeanStat
import com.eignex.kumulant.stat.quantile.BucketHistogramStat
import com.eignex.kumulant.stat.summary.MeanStat
import com.eignex.kumulant.stat.summary.SumStat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

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
        val buckets = BucketHistogramStat(doubleArrayOf(5.0, 15.0, 25.0))
        buckets.update(1.0, weight = -1.0)
        assertEquals(-1.0, buckets.read().weights.single(), 1e-9)
        buckets.update(1.0, weight = 1.0)
        assertEquals(0, buckets.read().weights.size)
    }

    @Test
    fun `Welford stats reject only the downdate that cannot be recovered`() {
        val mean = MeanStat()
        mean.update(10.0, weight = 2.0)
        mean.update(10.0, weight = -1.0)
        assertEquals(1.0, mean.read().totalWeights, 1e-9)
        assertFailsWith<IllegalArgumentException> { mean.update(10.0, weight = -1.0) }
        assertTrue(mean.read().mean.isFinite())
    }
}
