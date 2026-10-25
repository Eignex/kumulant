package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.stat.summary.BernoulliSumResult
import com.eignex.kumulant.stat.summary.MomentsResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
class BanditPoliciesTest {

    private fun <R : Result> drive(
        policy: BanditPolicy<R>,
        rewards: List<Double>,
        nbrArms: Int = 2,
    ): MultiArmedBandit<R> {
        val mab = MultiArmedBandit(nbrArms = nbrArms, policy = policy, random = Random(42))
        rewards.forEachIndexed { i, v -> mab.update(i % nbrArms, v) }
        return mab
    }

    @Test
    fun `positive reward policies produce positive finite samples`() {
        val cases = listOf(
            drive(LogNormalTS(), listOf(1.5, 2.0, 0.5, 3.0)),
            drive(PoissonTS(), listOf(2.0, 3.0, 1.0, 4.0)),
            drive(GammaScaleTS(fixedShape = 2.0), listOf(0.5, 1.0, 1.5, 2.0)),
        )

        for (bandit in cases) {
            repeat(20) {
                val score = bandit.evaluate(0)
                assertTrue(score.isFinite() && score > 0.0, "score=$score")
            }
        }
    }

    @Test
    fun `UCB1 returns infinity for untried arm and finite once played`() {
        val pol = UCB1(alpha = 1.0)
        val mab = MultiArmedBandit(nbrArms = 2, policy = pol, random = Random(0))
        val empty = BernoulliSumResult(0.0, 0.0)
        assertEquals(Double.POSITIVE_INFINITY, pol.evaluate(empty, 0L, Random(0)))
        mab.update(0, 1.0)
        mab.update(0, 0.0)
        mab.update(1, 1.0)
        assertTrue(mab.evaluate(0).isFinite())
        assertTrue(mab.evaluate(1).isFinite())
    }

    @Test
    fun `UCB1 addArm and removeArm adjust totalSamples`() {
        val pol = UCB1()
        val snap = BernoulliSumResult(successes = 3.0, trials = 10.0)
        pol.addArm(snap)
        pol.addArm(snap)
        val small = BernoulliSumResult(2.0, 4.0)
        val sBefore = pol.evaluate(small, 0L, Random(0))
        pol.removeArm(snap)
        val sAfter = pol.evaluate(small, 0L, Random(0))
        assertTrue(sBefore > sAfter, "expected sBefore=$sBefore > sAfter=$sAfter")
    }

    @Test
    fun `UCB1Normal returns infinity until enough samples`() {
        val pol = UCB1Normal()
        val mom = MomentsResult(
            totalWeights = 1.0,
            mean = 0.5,
            m2 = 0.1,
            m3 = 0.0,
            m4 = 0.0,
        )
        assertEquals(Double.POSITIVE_INFINITY, pol.evaluate(mom, 0L, Random(0)))
        pol.addArm(mom)
        pol.addArm(mom)
        pol.addArm(mom)
        assertEquals(Double.POSITIVE_INFINITY, pol.evaluate(mom, 0L, Random(0)))
        val big = mom.copy(totalWeights = 100.0, mean = 0.0, m2 = 25.0)
        assertTrue(pol.evaluate(big, 0L, Random(0)).isFinite())
        pol.removeArm(mom)
    }

    @Test
    fun `UCB1Tuned returns infinity when totalWeights le 1`() {
        val pol = UCB1Tuned()
        val mom = MomentsResult(1.0, 0.5, 0.0, 0.0, 0.0)
        assertEquals(Double.POSITIVE_INFINITY, pol.evaluate(mom, 0L, Random(0)))
        val bigger = mom.copy(totalWeights = 10.0, m2 = 1.0)
        pol.addArm(mom)
        pol.addArm(bigger)
        assertTrue(pol.evaluate(bigger, 0L, Random(0)).isFinite())
        pol.removeArm(mom)
    }

    @Test
    fun `EpsilonGreedy rejects out-of-range epsilon`() {
        assertFailsWith<IllegalArgumentException> { EpsilonGreedy(epsilon = -0.1) }
        assertFailsWith<IllegalArgumentException> { EpsilonGreedy(epsilon = 1.5) }
    }

    @Test
    fun `EpsilonDecreasing falls back to mean as samples grow`() {
        val pol = EpsilonDecreasing(epsilon = 2.0, decay = 0.5)
        val mab = MultiArmedBandit(nbrArms = 2, policy = pol, random = Random(0))
        repeat(200) {
            mab.update(0, 1.0)
            mab.update(1, 0.0)
        }
        var meanHits = 0
        repeat(100) { if (mab.evaluate(0) > 0.5) meanHits++ }
        assertTrue(meanHits > 50, "expected mostly exploit, got $meanHits/100")
    }

    @Test
    fun `BetaBernoulliTS mean approximates Beta a over a_plus_b`() {
        val pol = BetaBernoulliTS(priorAlpha = 5.0, priorBeta = 2.0)
        val mab = MultiArmedBandit(nbrArms = 1, policy = pol, random = Random(11))
        var sum = 0.0
        val n = 4000
        repeat(n) { sum += mab.evaluate(0) }
        val mean = sum / n
        assertTrue(abs(mean - 5.0 / 7.0) < 0.03, "mean=$mean")
    }
}

class EpsilonPolicyRandomnessTest {

    private val snapshot = WeightedVarianceResult(totalWeights = 10.0, mean = 5.0, variance = 1.0)

    @Test
    fun `the explore decision depends on the caller's rng`() {
        val decisions = (1..50).map { seed ->
            EpsilonGreedy(epsilon = 0.5).evaluate(snapshot, step = 7L, rng = Random(seed)) == snapshot.mean
        }
        assertTrue(decisions.toSet().size > 1, "the decision at a fixed step is the same for every seed")
    }

    @Test
    fun `the explore decision is shared by every arm in a round`() {
        for (seed in 1..20) {
            val policy = EpsilonGreedy(epsilon = 0.5)
            val rng = Random(seed)
            val first = policy.evaluate(snapshot, step = 7L, rng = rng)
            val second = policy.evaluate(snapshot, step = 7L, rng = rng)
            assertEquals(
                first == snapshot.mean,
                second == snapshot.mean,
                "arms in one round disagreed on exploring, seed=$seed",
            )
        }
    }
}
