package com.eignex.kumulant.bandit.univariate

import com.eignex.kumulant.stat.summary.BernoulliSumResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.math.ln
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
        assertEquals(-ln(1.0 - 0.3), KlUcb.klBernoulli(0.0, 0.3), 1e-12)
        assertEquals(-ln(0.4), KlUcb.klBernoulli(1.0, 0.4), 1e-12)
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
    fun `MOSS drives MultiArmedBandit to the best arm`() {
        val rng = Random(2)
        val bandit = MultiArmedBandit(nbrArms = 3, policy = Moss(), random = rng)
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
    fun `annealed EpsilonGreedy rejects a negative decay`() {
        assertFailsWith<IllegalArgumentException> { EpsilonGreedy(epsilon = 2.0, decay = -0.5) }
    }

    @Test
    fun `KL-UCB returns infinity when totalSamples is too small`() {
        val pol = KlUcb()
        val snap = BernoulliSumResult(successes = 3.0, trials = 10.0)
        assertEquals(Double.POSITIVE_INFINITY, pol.evaluate(snap, 0L, Random(0)))
    }

    @Test
    fun `MOSS returns infinity for untried arm`() {
        val pol = Moss()
        val snap = WeightedMeanResult(0.0, 0.0)
        assertEquals(Double.POSITIVE_INFINITY, pol.evaluate(snap, 0L, Random(0)))
    }

    @Test
    fun `UCB-V returns infinity for untried arm`() {
        val pol = UcbV()
        val snap = WeightedVarianceResult(0.0, 0.0, 0.0)
        assertEquals(Double.POSITIVE_INFINITY, pol.evaluate(snap, 0L, Random(0)))
    }

    @Test
    fun `UCB-V score grows with empirical variance at fixed mean`() {
        val pol = UcbV(zeta = 1.2, c = 1.0)
        repeat(100) { pol.update(pol.arm.createStat(), 1.0) }
        val low = WeightedVarianceResult(totalWeights = 10.0, mean = 1.0, variance = 1.0)
        val high = WeightedVarianceResult(totalWeights = 10.0, mean = 1.0, variance = 3.0)
        assertTrue(pol.evaluate(high, 0L, Random(0)) > pol.evaluate(low, 0L, Random(0)))
    }
}
