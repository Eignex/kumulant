package com.eignex.kumulant.bandit

import com.eignex.kumulant.stat.summary.MomentsResult
import kotlin.math.abs
import kotlin.math.ln
import kotlin.test.Test
import kotlin.test.assertEquals
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
    fun `MomentsArm seeds mean prior`() {
        val arm = MomentsArm(priorMean = 1.5, priorWeight = 4.0)
        val r = arm.createStat().read(0L)
        assertEquals(4.0, r.totalWeights)
        assertEquals(1.5, r.mean)
    }

    @Test
    fun `MomentsArm zero priorWeight is a no-op`() {
        val r = MomentsArm(priorMean = 0.0, priorWeight = 0.0).createStat().read(0L)
        assertEquals(0.0, r.totalWeights)
    }

    @Test
    fun `meanOfSquares is zero for empty snapshot and positive otherwise`() {
        val empty = MomentsResult(0.0, 0.0, 0.0, 0.0, 0.0)
        assertEquals(0.0, empty.meanOfSquares())
        val populated = MomentsResult(totalWeights = 4.0, mean = 2.0, m2 = 8.0, m3 = 0.0, m4 = 0.0)
        assertEquals(6.0, populated.meanOfSquares())
    }

    @Test
    fun `BernoulliArm Arm encode is identity`() {
        val arm: Arm<*> = BernoulliArm()
        assertEquals(0.5, arm.encode(0.5))
    }

    @Test
    fun `BernoulliArm warmStart copies scaled successes and failures from global`() {
        val global = com.eignex.kumulant.stat.summary.BernoulliSumResult(successes = 30.0, trials = 50.0)
        val arm = BernoulliArm.warmStart(global, shrinkage = 0.5)
        assertEquals(15.0, arm.priorAlpha)
        assertEquals(10.0, arm.priorBeta)
    }

    @Test
    fun `BernoulliArm warmStart with shrinkage zero produces zero pseudo-counts`() {
        val global = com.eignex.kumulant.stat.summary.BernoulliSumResult(successes = 30.0, trials = 50.0)
        val arm = BernoulliArm.warmStart(global, shrinkage = 0.0)
        assertEquals(0.0, arm.priorAlpha)
        assertEquals(0.0, arm.priorBeta)
    }

    @Test
    fun `BernoulliArm warmStart rejects shrinkage outside the unit interval`() {
        val global = com.eignex.kumulant.stat.summary.BernoulliSumResult(successes = 1.0, trials = 2.0)
        kotlin.test.assertFailsWith<IllegalArgumentException> {
            BernoulliArm.warmStart(global, shrinkage = -0.1)
        }
        kotlin.test.assertFailsWith<IllegalArgumentException> {
            BernoulliArm.warmStart(global, shrinkage = 1.1)
        }
    }

    @Test
    fun `MeanArm warmStart uses global mean and shrunk weight`() {
        val global = com.eignex.kumulant.stat.summary.WeightedMeanResult(totalWeights = 100.0, mean = 7.0)
        val arm = MeanArm.warmStart(global, shrinkage = 0.3)
        assertEquals(7.0, arm.priorMean)
        assertEquals(30.0, arm.priorWeight)
    }

    @Test
    fun `NormalArm warmStart preserves variance and shrinks weight`() {
        val global = com.eignex.kumulant.stat.summary.WeightedVarianceResult(
            totalWeights = 200.0,
            mean = 4.0,
            variance = 9.0,
        )
        val arm = NormalArm.warmStart(global, shrinkage = 0.5)
        assertEquals(4.0, arm.priorMean)
        assertEquals(100.0, arm.priorWeight)
        // priorSquaredDeviations = variance * shrunkWeight = 9 * 100 = 900
        assertEquals(900.0, arm.priorSquaredDeviations)
    }

    @Test
    fun `LogNormalArm warmStart matches NormalArm shape on log-scale snapshot`() {
        val global = com.eignex.kumulant.stat.summary.WeightedVarianceResult(
            totalWeights = 80.0,
            mean = 1.5,
            variance = 4.0,
        )
        val arm = LogNormalArm.warmStart(global, shrinkage = 0.25)
        assertEquals(1.5, arm.priorMean)
        assertEquals(20.0, arm.priorWeight)
        assertEquals(80.0, arm.priorSquaredDeviations)
    }

    @Test
    fun `MomentsArm warmStart uses global mean and shrunk weight`() {
        val global = MomentsResult(
            totalWeights = 50.0,
            mean = 3.0,
            m2 = 100.0,
            m3 = 0.0,
            m4 = 0.0,
        )
        val arm = MomentsArm.warmStart(global, shrinkage = 0.8)
        assertEquals(3.0, arm.priorMean)
        assertEquals(40.0, arm.priorWeight)
    }
}
