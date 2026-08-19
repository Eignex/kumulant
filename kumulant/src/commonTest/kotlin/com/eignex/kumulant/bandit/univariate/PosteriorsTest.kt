package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.stat.summary.BernoulliSumResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertTrue

class PosteriorsTest {

    @Test
    fun `BetaPosterior samples are in 0_1`() {
        val snap = BernoulliSumResult(successes = 4.0, trials = 10.0)
        val rng = Random(1)
        var sum = 0.0
        val n = 3000
        repeat(n) {
            val s = BetaPosterior.sample(snap, rng)
            assertTrue(s in 0.0..1.0)
            sum += s
        }
        assertTrue(abs(sum / n - 0.4) < 0.03)
    }

    @Test
    fun `PoissonGammaPosterior samples are positive finite`() {
        val snap = WeightedMeanResult(totalWeights = 8.0, mean = 2.5)
        val rng = Random(2)
        var sum = 0.0
        val n = 3000
        repeat(n) {
            val s = PoissonGammaPosterior.sample(snap, rng)
            assertTrue(s.isFinite() && s > 0.0)
            sum += s
        }
        assertTrue(abs(sum / n - 2.5) < 0.15)
    }

    @Test
    fun `GeometricBetaPosterior samples center on the observed trial count`() {
        val snap = WeightedMeanResult(totalWeights = 400.0, mean = 3.0)
        val rng = Random(3)
        var sum = 0.0
        val n = 3000
        repeat(n) {
            val s = GeometricBetaPosterior.sample(snap, rng)
            assertTrue(s.isFinite() && s > 1.0)
            sum += s
        }
        assertTrue(abs(sum / n - 3.0) < 0.15)
    }

    @Test
    fun `ExponentialGammaPosterior samples center on the observed reward mean`() {
        val snap = WeightedMeanResult(totalWeights = 200.0, mean = 2.0)
        val rng = Random(4)
        var sum = 0.0
        val n = 3000
        repeat(n) {
            val s = ExponentialGammaPosterior.sample(snap, rng)
            assertTrue(s.isFinite() && s > 0.0)
            sum += s
        }
        assertTrue(abs(sum / n - 2.0) < 0.1)
    }

    @Test
    fun `NormalGammaPosterior samples are finite and centered on snapshot mean`() {
        val snap = WeightedVarianceResult(totalWeights = 30.0, mean = 1.5, variance = 0.5)
        val rng = Random(5)
        var sum = 0.0
        val n = 3000
        repeat(n) {
            val s = NormalGammaPosterior.sample(snap, rng)
            assertTrue(s.isFinite())
            sum += s
        }
        assertTrue(abs(sum / n - 1.5) < 0.15)
    }

    @Test
    fun `LogNormalGammaPosterior samples are positive finite`() {
        val snap = WeightedVarianceResult(totalWeights = 20.0, mean = 0.0, variance = 0.5)
        val rng = Random(6)
        repeat(500) {
            val s = LogNormalGammaPosterior.sample(snap, rng)
            assertTrue(s.isFinite() && s > 0.0, "got $s")
        }
    }

    @Test
    fun `GammaScalePosterior samples center on the observed reward mean`() {
        val pos = GammaScalePosterior(fixedShape = 2.0)
        val snap = WeightedMeanResult(totalWeights = 200.0, mean = 1.0)
        val rng = Random(7)
        var sum = 0.0
        val n = 2000
        repeat(n) {
            val s = pos.sample(snap, rng)
            assertTrue(s.isFinite() && s > 0.0)
            sum += s
        }
        assertTrue(abs(sum / n - 1.0) < 0.1)
    }

    @Test
    fun `ExponentialTS prefers the arm with the larger reward`() {
        val mab = MultiArmedBandit(nbrArms = 2, policy = ExponentialTS(), random = Random(11))
        repeat(200) {
            mab.update(0, 10.0)
            mab.update(1, 1.0)
        }
        val picks = (0 until 1000).count { mab.choose() == 0 }
        assertTrue(picks > 900, "arm 0 chosen $picks/1000")
    }

    @Test
    fun `GeometricTS prefers the arm with the larger reward`() {
        val mab = MultiArmedBandit(nbrArms = 2, policy = GeometricTS(), random = Random(12))
        repeat(200) {
            mab.update(0, 8.0)
            mab.update(1, 2.0)
        }
        val picks = (0 until 1000).count { mab.choose() == 0 }
        assertTrue(picks > 900, "arm 0 chosen $picks/1000")
    }
}
