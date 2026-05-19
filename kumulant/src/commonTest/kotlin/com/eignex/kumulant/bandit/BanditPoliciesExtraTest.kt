package com.eignex.kumulant.bandit

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
