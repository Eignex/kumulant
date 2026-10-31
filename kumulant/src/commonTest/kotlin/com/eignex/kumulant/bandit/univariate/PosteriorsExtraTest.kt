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

}
