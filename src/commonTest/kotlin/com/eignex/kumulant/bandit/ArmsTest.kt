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
}
