package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.DELTA
import com.eignex.kumulant.stat.summary.BernoulliSumResult
import com.eignex.kumulant.stat.summary.MomentsResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.math.abs
import kotlin.math.ln
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
class ArmsTest {

    @Test
    fun `BernoulliArm seeds prior pseudo-counts`() {
        val arm = BernoulliArm(priorAlpha = 3.0, priorBeta = 5.0)
        val r = arm.createStat().read(0L)
        assertEquals(3.0, r.successes)
        assertEquals(8.0, r.trials)
    }

    @Test
    fun `BernoulliArm skips zero priors`() {
        val arm = BernoulliArm(priorAlpha = 0.0, priorBeta = 0.0)
        val r = arm.createStat().read(0L)
        assertEquals(0.0, r.successes)
        assertEquals(0.0, r.trials)
    }

    @Test
    fun `MeanArm seeds prior pseudo-counts`() {
        val arm = MeanArm(priorMean = 2.5, priorWeight = 4.0)
        val r = arm.createStat().read(0L)
        assertEquals(4.0, r.totalWeights)
        assertEquals(2.5, r.mean)
    }

    @Test
    fun `MeanArm zero priorWeight is a no-op`() {
        val arm = MeanArm(priorMean = 99.0, priorWeight = 0.0)
        val r = arm.createStat().read(0L)
        assertEquals(0.0, r.totalWeights)
    }

    @Test
    fun `NormalArm seeds three-point prior so variance is positive`() {
        val arm = NormalArm(priorMean = 1.0, priorWeight = 2.0, priorSquaredDeviations = 8.0)
        val r = arm.createStat().read(0L)
        assertEquals(4.0, r.totalWeights)
        assertTrue(abs(r.mean - 1.0) < 1e-9)
        assertTrue(r.variance > 0.0, "variance=${r.variance}")
    }

    @Test
    fun `NormalArm with zero priorSquaredDeviations still seeds the mean`() {
        val arm = NormalArm(priorMean = 1.0, priorWeight = 2.0, priorSquaredDeviations = 0.0)
        val r = arm.createStat().read(0L)
        assertEquals(2.0, r.totalWeights)
        assertEquals(1.0, r.mean)
    }

    @Test
    fun `NormalArm zero priorWeight is a no-op`() {
        val arm = NormalArm(priorMean = 5.0, priorWeight = 0.0)
        val r = arm.createStat().read(0L)
        assertEquals(0.0, r.totalWeights)
    }

    @Test
    fun `LogNormalArm encodes via ln`() {
        val arm = LogNormalArm()
        assertEquals(ln(2.0), arm.encode(2.0))
    }

    @Test
    fun `LogNormalArm createStat matches NormalArm with same priors`() {
        val a = LogNormalArm(0.0, 0.5, 1.0).createStat().read(0L)
        val b = NormalArm(0.0, 0.5, 1.0).createStat().read(0L)
        assertEquals(b, a)
    }

    @Test
    fun `BernoulliArm Arm encode is identity`() {
        val arm: Arm<*> = BernoulliArm()
        assertEquals(0.5, arm.encode(0.5))
    }

    @Test
    fun `merging a fresh NormalArm snapshot leaves the seeded prior unchanged`() {
        val arm = NormalArm(priorMean = 1.0, priorWeight = 2.0, priorSquaredDeviations = 8.0)
        val stat = arm.createStat()

        stat.merge(arm.createStat().read(0L))

        val expected = arm.createStat().read(0L)
        val actual = stat.read(0L)
        assertEquals(expected.totalWeights, actual.totalWeights, DELTA)
        assertEquals(expected.mean, actual.mean, DELTA)
        assertEquals(expected.variance, actual.variance, DELTA)
    }

    @Test
    fun `merging an empty snapshot into a seeded NormalArm stat keeps its prior`() {
        val arm = NormalArm(priorMean = 1.0, priorWeight = 2.0, priorSquaredDeviations = 8.0)
        val stat = arm.createStat()

        stat.merge(WeightedVarianceResult(0.0, 0.0, 0.0))

        assertEquals(arm.createStat().read(0L), stat.read(0L))
    }
}
