package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.stat.summary.BernoulliSumResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PosteriorsExtraTest {

    @Test
    fun `BetaPosterior is deterministic under a fixed seed`() {
        val snap = BernoulliSumResult(successes = 5.0, trials = 10.0)
        val a = BetaPosterior.sample(snap, Random(42))
        val b = BetaPosterior.sample(snap, Random(42))
        assertEquals(a, b)
    }

    @Test
    fun `BetaPosterior concentrates around the empirical mean for large samples`() {
        val snap = BernoulliSumResult(successes = 600.0, trials = 1000.0)
        val rng = Random(0)
        val draws = DoubleArray(200) { BetaPosterior.sample(snap, rng) }
        val mean = draws.average()
        assertTrue(abs(mean - 0.6) < 0.05, "expected mean near 0.6, got $mean")
    }

    @Test
    fun `BetaPosterior reflects priors from BernoulliArm`() {
        // Sampling from a prior-only snapshot stays bounded in [0, 1]; use BernoulliArm
        // priors > 0 so the underlying nextBeta doesn't see alpha = 0.
        val snap = BernoulliArm(priorAlpha = 1.0, priorBeta = 1.0).createStat().read(0L)
        val rng = Random(1)
        val s = BetaPosterior.sample(snap, rng)
        assertTrue(s in 0.0..1.0)
    }

    @Test
    fun `NormalGammaPosterior centers near the snapshot mean for high count`() {
        val snap = WeightedVarianceResult(totalWeights = 100.0, mean = 5.0, variance = 1.0)
        val rng = Random(2)
        val draws = DoubleArray(200) { NormalGammaPosterior.sample(snap, rng) }
        val avg = draws.average()
        assertTrue(abs(avg - 5.0) < 0.5, "expected mean near 5, got $avg")
    }

    @Test
    fun `LogNormalGammaPosterior samples are positive`() {
        val snap = WeightedVarianceResult(totalWeights = 50.0, mean = 1.0, variance = 0.25)
        val rng = Random(3)
        repeat(50) {
            val s = LogNormalGammaPosterior.sample(snap, rng)
            assertTrue(s > 0.0, "got non-positive sample: $s")
        }
    }

    @Test
    fun `PoissonGammaPosterior samples are positive given a non-empty snapshot`() {
        // Seed with a small pseudo-count so the Gamma sampler has alpha > 0.
        val snap = MeanArm(priorMean = 1.0, priorWeight = 0.5).createStat().also {
            it.update(2.0)
        }.read(0L)
        val rng = Random(4)
        repeat(30) {
            val s = PoissonGammaPosterior.sample(snap, rng)
            assertTrue(s >= 0.0 && s.isFinite(), "got $s")
        }
    }

    @Test
    fun `GeometricBetaPosterior samples exceed one trial`() {
        val snap = WeightedMeanResult(totalWeights = 20.0, mean = 2.0)
        val rng = Random(5)
        repeat(50) {
            val s = GeometricBetaPosterior.sample(snap, rng)
            assertTrue(s.isFinite() && s > 1.0, "got $s")
        }
    }

    @Test
    fun `ExponentialGammaPosterior samples are positive`() {
        val snap = WeightedMeanResult(totalWeights = 30.0, mean = 2.0)
        val rng = Random(6)
        repeat(50) {
            val s = ExponentialGammaPosterior.sample(snap, rng)
            assertTrue(s > 0.0, "got $s")
        }
    }

    @Test
    fun `GammaScalePosterior fixedShape input is respected by sampler`() {
        val snap = WeightedMeanResult(totalWeights = 50.0, mean = 2.0)
        val rng = Random(7)
        val pos1 = GammaScalePosterior(fixedShape = 1.0)
        val pos2 = GammaScalePosterior(fixedShape = 5.0)
        // Same snapshot, different shapes -> different distribution; not equal almost surely.
        val a = pos1.sample(snap, rng)
        val b = pos2.sample(snap, Random(7))
        assertTrue(a != b)
        assertTrue(a > 0.0 && b > 0.0)
    }
}
