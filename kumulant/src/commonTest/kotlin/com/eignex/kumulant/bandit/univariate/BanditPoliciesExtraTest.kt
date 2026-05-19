package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.stat.summary.BernoulliSumResult
import com.eignex.kumulant.stat.summary.MomentsResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class BanditPoliciesExtraTest {

    @Test
    fun `klBernoulli is zero for matching interior means`() {
        assertEquals(0.0, KlUcb.klBernoulli(0.5, 0.5), 1e-12)
        assertEquals(0.0, KlUcb.klBernoulli(0.3, 0.3), 1e-12)
        assertEquals(0.0, KlUcb.klBernoulli(0.7, 0.7), 1e-12)
    }

    @Test
    fun `klBernoulli handles boundary p values`() {
        assertEquals(-kotlin.math.ln(1.0 - 0.3), KlUcb.klBernoulli(0.0, 0.3), 1e-12)
        assertEquals(-kotlin.math.ln(0.4), KlUcb.klBernoulli(1.0, 0.4), 1e-12)
    }

    @Test
    fun `klBernoulli is infinite when q hits zero or one and p disagrees`() {
        assertTrue(KlUcb.klBernoulli(0.5, 0.0).isInfinite())
        assertTrue(KlUcb.klBernoulli(0.5, 1.0).isInfinite())
    }

    @Test
    fun `klBernoulliUpper collapses to p when bound is zero or negative`() {
        assertEquals(0.5, KlUcb.klBernoulliUpper(0.5, bound = 0.0, tol = 1e-6))
        assertEquals(0.5, KlUcb.klBernoulliUpper(0.5, bound = -0.1, tol = 1e-6))
    }

    @Test
    fun `klBernoulliUpper grows monotonically with the bound`() {
        val low = KlUcb.klBernoulliUpper(0.5, bound = 0.05, tol = 1e-6)
        val mid = KlUcb.klBernoulliUpper(0.5, bound = 0.2, tol = 1e-6)
        val high = KlUcb.klBernoulliUpper(0.5, bound = 1.0, tol = 1e-6)
        assertTrue(low < mid)
        assertTrue(mid < high)
        assertTrue(high <= 1.0)
    }

    @Test
    fun `klBernoulliUpper stays in zero one`() {
        val q = KlUcb.klBernoulliUpper(0.99, bound = 5.0, tol = 1e-6)
        assertTrue(q in 0.99..1.0, "q=$q")
    }

    @Test
    fun `KL-UCB Bernoulli KL is symmetric only at the corners`() {
        assertEquals(0.0, KlUcb.klBernoulli(0.3, 0.3), 1e-9)
        assertTrue(KlUcb.klBernoulli(0.1, 0.5) > KlUcb.klBernoulli(0.5, 0.5))
    }

    @Test
    fun `KL-UCB upper bound exceeds the mean`() {
        val q = KlUcb.klBernoulliUpper(p = 0.5, bound = 0.1, tol = 1e-6)
        assertTrue(q > 0.5 && q < 1.0)
    }

    @Test
    fun `KL-UCB drives MultiArmedBandit to the best Bernoulli arm`() {
        val rng = Random(1)
        val bandit = MultiArmedBandit(nbrArms = 3, policy = KlUcb(), random = rng)
        val ps = doubleArrayOf(0.2, 0.7, 0.4)
        repeat(2000) {
            val a = bandit.choose()
            val r = if (rng.nextDouble() < ps[a]) 1.0 else 0.0
            bandit.update(a, r)
        }
        val picks = IntArray(3)
        repeat(300) { picks[bandit.choose()]++ }
        assertTrue(picks[1] > picks[0] && picks[1] > picks[2], "arm 1 should dominate: ${picks.toList()}")
    }

    @Test
    fun `MOSS rejects non-positive nbrArms`() {
        assertFailsWith<IllegalArgumentException> { Moss(nbrArms = 0) }
    }

    @Test
    fun `MOSS drives MultiArmedBandit to the best arm`() {
        val rng = Random(2)
        val bandit = MultiArmedBandit(nbrArms = 3, policy = Moss(nbrArms = 3), random = rng)
        repeat(1000) {
            val a = bandit.choose()
            val reward = when (a) {
                0 -> 0.1
                1 -> 0.7
                else -> 0.3
            } + rng.nextDouble() * 0.05
            bandit.update(a, reward)
        }
        val picks = IntArray(3)
        repeat(300) { picks[bandit.choose()]++ }
        assertTrue(picks[1] > picks[0] && picks[1] > picks[2], "arm 1 should dominate: ${picks.toList()}")
    }

    @Test
    fun `UCB-V rejects non-positive zeta`() {
        assertFailsWith<IllegalArgumentException> { UcbV(zeta = 0.0) }
    }

    @Test
    fun `UCB-V drives MultiArmedBandit to the best arm`() {
        val rng = Random(3)
        val bandit = MultiArmedBandit(nbrArms = 3, policy = UcbV(), random = rng)
        repeat(1000) {
            val a = bandit.choose()
            val mean = when (a) {
                0 -> 0.2
                1 -> 0.8
                else -> 0.5
            }
            bandit.update(a, mean + rng.nextDouble() * 0.1)
        }
        val picks = IntArray(3)
        repeat(300) { picks[bandit.choose()]++ }
        assertTrue(picks[1] > picks[0] && picks[1] > picks[2], "arm 1 should dominate: ${picks.toList()}")
    }

    @Test
    fun `UniformSelection draws every score from rng without inspecting snapshot`() {
        val pol = UniformSelection()
        val snap = WeightedVarianceResult(10.0, 999.0, 1.0)
        val rng = Random(0)
        val a = pol.evaluate(snap, 0L, rng)
        val b = pol.evaluate(snap, 0L, rng)
        assertTrue(a in 0.0..1.0 && b in 0.0..1.0)
        assertTrue(a != b, "two draws should differ under non-degenerate rng")
    }

    @Test
    fun `Greedy returns snapshot mean ignoring rng`() {
        val pol = Greedy()
        val snap = WeightedVarianceResult(5.0, 7.0, 0.1)
        assertEquals(7.0, pol.evaluate(snap, 0L, Random(0)))
        assertEquals(7.0, pol.evaluate(snap, 100L, Random(999)))
    }

    @Test
    fun `EpsilonGreedy at zero epsilon always exploits`() {
        val pol = EpsilonGreedy(epsilon = 0.0)
        val snap = WeightedVarianceResult(1.0, 3.0, 0.0)
        repeat(20) {
            assertEquals(3.0, pol.evaluate(snap, it.toLong(), Random(0)))
        }
    }

    @Test
    fun `EpsilonGreedy at one always explores uniformly`() {
        val pol = EpsilonGreedy(epsilon = 1.0)
        val snap = WeightedVarianceResult(1.0, 999.0, 0.0)
        for (step in 0L until 20L) {
            val s = pol.evaluate(snap, step, Random(0))
            assertTrue(s in 0.0..1.0, "got $s")
        }
    }

    @Test
    fun `EpsilonDecreasing rejects bad params`() {
        assertFailsWith<IllegalArgumentException> { EpsilonDecreasing(epsilon = -0.1) }
        assertFailsWith<IllegalArgumentException> { EpsilonDecreasing(epsilon = 0.0) }
    }

    @Test
    fun `UCB1 infinite for untried, finite once tried`() {
        val pol = UCB1()
        val empty = BernoulliSumResult(0.0, 0.0)
        assertEquals(Double.POSITIVE_INFINITY, pol.evaluate(empty, 0L, Random(0)))
        pol.addArm(BernoulliSumResult(0.0, 0.0))
        val populated = BernoulliSumResult(3.0, 10.0)
        pol.addArm(populated)
        val s = pol.evaluate(populated, 0L, Random(0))
        assertTrue(s.isFinite() && s > 0.3)
    }

    @Test
    fun `UCB1Normal forces exploration when nj is small`() {
        val pol = UCB1Normal()
        pol.addArm(MomentsResult(0.0, 0.0, 0.0, 0.0, 0.0))
        pol.addArm(MomentsResult(0.0, 0.0, 0.0, 0.0, 0.0))
        val tiny = MomentsResult(totalWeights = 1.0, mean = 1.0, m2 = 1.0, m3 = 0.0, m4 = 0.0)
        // nj=1 is below the forced-exploration threshold ceil(8 ln(K))
        assertEquals(Double.POSITIVE_INFINITY, pol.evaluate(tiny, 0L, Random(0)))
    }

    @Test
    fun `UCB1Tuned grows bound with totalSamples`() {
        val pol = UCB1Tuned()
        pol.update(pol.arm.createStat(), 1.0)
        repeat(100) { pol.update(pol.arm.createStat(), 1.0) }
        val snap = MomentsResult(totalWeights = 2.0, mean = 0.5, m2 = 0.5, m3 = 0.0, m4 = 0.0)
        val s = pol.evaluate(snap, 0L, Random(0))
        assertTrue(s > 0.5 && s.isFinite())
    }

    @Test
    fun `KL-UCB returns infinity when totalSamples is too small`() {
        val pol = KlUcb()
        val snap = BernoulliSumResult(successes = 3.0, trials = 10.0)
        // No samples folded in via update -> totalSamples = 0
        assertEquals(Double.POSITIVE_INFINITY, pol.evaluate(snap, 0L, Random(0)))
    }

    @Test
    fun `MOSS returns infinity for untried arm`() {
        val pol = Moss(nbrArms = 3)
        val snap = WeightedMeanResult(0.0, 0.0)
        assertEquals(Double.POSITIVE_INFINITY, pol.evaluate(snap, 0L, Random(0)))
    }

    @Test
    fun `UCB-V returns infinity for untried arm`() {
        val pol = UcbV()
        val snap = MomentsResult(0.0, 0.0, 0.0, 0.0, 0.0)
        assertEquals(Double.POSITIVE_INFINITY, pol.evaluate(snap, 0L, Random(0)))
    }

    @Test
    fun `UCB-V score grows with empirical variance at fixed mean`() {
        val pol = UcbV(zeta = 1.2, c = 1.0)
        // populate totalSamples so the bound has logT > 0
        repeat(100) { pol.update(pol.arm.createStat(), 1.0) }
        val low = MomentsResult(totalWeights = 10.0, mean = 1.0, m2 = 10.0, m3 = 0.0, m4 = 0.0) // var = 0
        val high = MomentsResult(totalWeights = 10.0, mean = 1.0, m2 = 30.0, m3 = 0.0, m4 = 0.0) // var = 2
        assertTrue(pol.evaluate(high, 0L, Random(0)) > pol.evaluate(low, 0L, Random(0)))
    }
}
