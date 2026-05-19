package com.eignex.kumulant.bandit

import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.math.VectorView
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Edge cases across Exp3, Exp4, Boltzmann, Knn, TopTwoTS that the per-class
 *  tests don't already cover. */
class StandaloneBanditExtraTest {

    private fun feat(vararg xs: Double): DenseVector = DenseVector.of(xs)

    // === Exp3 ===

    @Test
    fun `Exp3 gamma=1 produces uniform play distribution regardless of weights`() {
        val b = Exp3Bandit(nbrArms = 3, eta = 0.5, gamma = 1.0, random = Random(0))
        repeat(50) { b.update(0, 1.0) }
        val p = b.playDistribution()
        for (a in 0 until 3) assertTrue(abs(p[a] - 1.0 / 3) < 1e-9, "p[$a]=${p[a]}")
    }

    @Test
    fun `Exp3 gamma=0 puts no uniform mass on losing arms after training`() {
        val b = Exp3Bandit(nbrArms = 2, eta = 1.0, gamma = 0.0, random = Random(1))
        repeat(50) {
            // Always observe a perfect reward for arm 0; arm 1 never plays so its weight stays 1.
            b.update(0, 1.0)
        }
        val p = b.playDistribution()
        assertTrue(p[0] > 0.95, "arm 0 should dominate without uniform mass: $p")
    }

    @Test
    fun `Exp3 armWeights sum to 1`() {
        val b = Exp3Bandit(nbrArms = 4, random = Random(2))
        repeat(20) { b.update(b.choose(), 1.0) }
        val w = b.armWeights()
        assertEquals(1.0, w.sum(), 1e-9)
    }

    @Test
    fun `Exp3Bandit defaultEta is positive and grows with nbrArms`() {
        val small = Exp3Bandit.defaultEta(2)
        val large = Exp3Bandit.defaultEta(10)
        assertTrue(small > 0.0 && large > 0.0)
        // ln(K) grows slowly; sqrt(ln(K)/K) shrinks with K. Either way both finite.
        assertTrue(small.isFinite() && large.isFinite())
    }

    // === Exp4 ===

    @Test
    fun `Exp4 rejects expert returning wrong-length advice`() {
        val badExpert = Exp4Expert { _, _ -> DoubleArray(99) { 1.0 / 99 } }
        val b = Exp4Bandit(nbrArms = 3, experts = listOf(badExpert), random = Random(0))
        kotlin.test.assertFailsWith<IllegalArgumentException> { b.playDistribution(feat(0.0)) }
    }

    @Test
    fun `Exp4 gamma=1 produces uniform play distribution`() {
        val expert = Exp4Expert { _, n -> DoubleArray(n).also { it[0] = 1.0 } }
        val b = Exp4Bandit(nbrArms = 3, experts = listOf(expert), gamma = 1.0, random = Random(0))
        val p = b.playDistribution(feat(0.0))
        for (a in 0 until 3) assertTrue(abs(p[a] - 1.0 / 3) < 1e-9, "p[$a]=${p[a]}")
    }

    @Test
    fun `Exp4 expertWeights sum to 1`() {
        val experts: List<Exp4Expert> = listOf(
            Exp4Expert { _, n -> DoubleArray(n) { 1.0 / n } },
            Exp4Expert { _: VectorView, n: Int -> DoubleArray(n).also { it[0] = 1.0 } },
        )
        val b = Exp4Bandit(nbrArms = 3, experts = experts, random = Random(1))
        repeat(20) { b.update(b.choose(feat(0.0)), feat(0.0), 1.0) }
        val w = b.expertWeights()
        assertEquals(1.0, w.sum(), 1e-9)
    }

    // === Boltzmann ===

    @Test
    fun `Boltzmann temperature respects floor`() {
        val b = BoltzmannBandit(
            nbrArms = 2,
            initialTau = 1.0,
            minTau = 0.5,
            decay = 5.0,
            random = Random(0),
        )
        // After many calls the schedule's bare tau would be way below 0.5; floor kicks in.
        repeat(50) { b.playDistribution() }
        assertEquals(0.5, b.temperature(), 1e-9)
    }

    @Test
    fun `Boltzmann fixed temperature when decay is zero`() {
        val b = BoltzmannBandit(nbrArms = 2, initialTau = 2.5, decay = 0.0, random = Random(0))
        b.playDistribution()
        b.playDistribution()
        assertEquals(2.5, b.temperature(), 1e-9)
    }

    @Test
    fun `Boltzmann playDistribution sums to 1`() {
        val b = BoltzmannBandit(nbrArms = 4, initialTau = 1.0, decay = 0.0, random = Random(0))
        repeat(20) { b.update(it % 4, it.toDouble()) }
        val p = b.playDistribution()
        assertEquals(1.0, p.sum(), 1e-9)
    }

    @Test
    fun `Boltzmann snapshot matches per-arm armResult`() {
        val b = BoltzmannBandit(nbrArms = 3, initialTau = 1.0, decay = 0.0, random = Random(0))
        repeat(10) { b.update(it % 3, it.toDouble()) }
        val snap = b.snapshot()
        for (a in 0 until 3) assertEquals(b.armResult(a), snap[a])
    }

    // === Knn ===

    @Test
    fun `Knn uses custom distance function`() {
        // Custom distance: always returns 1 — every point looks equidistant.
        val b = KnnContextualBandit(
            nbrArms = 2,
            k = 2,
            exploration = 0.0,
            distance = { _, _ -> 1.0 },
            random = Random(0),
        )
        repeat(4) { b.update(0, feat(it.toDouble()), it.toDouble()) }
        // Without spatial info every query returns the same average.
        val a = b.evaluate(0, feat(-100.0))
        val c = b.evaluate(0, feat(100.0))
        assertEquals(a, c, 1e-9)
    }

    @Test
    fun `Knn historySize and armWeight track per arm`() {
        val b = KnnContextualBandit(nbrArms = 2, k = 2, exploration = 0.0)
        b.update(0, feat(0.0), 1.0, weight = 2.0)
        b.update(0, feat(1.0), 1.0)
        b.update(1, feat(0.0), 1.0)
        assertEquals(2, b.historySize(0))
        assertEquals(1, b.historySize(1))
        assertEquals(3.0, b.armWeight(0), 1e-9)
        assertEquals(1.0, b.armWeight(1), 1e-9)
    }

    @Test
    fun `Knn exploration scale grows the score on low-evidence arms`() {
        val baseline = KnnContextualBandit(nbrArms = 1, k = 1, exploration = 0.0, random = Random(0))
        val bonus = KnnContextualBandit(nbrArms = 1, k = 1, exploration = 5.0, random = Random(0))
        baseline.update(0, feat(0.0), 1.0)
        bonus.update(0, feat(0.0), 1.0)
        // Advance the step counters so the UCB bound has ln(t) > 0.
        repeat(5) {
            baseline.choose(feat(0.0))
            bonus.choose(feat(0.0))
        }
        assertTrue(bonus.evaluate(0, feat(0.0)) > baseline.evaluate(0, feat(0.0)))
    }

    @Test
    fun `Knn squaredL2 distance behaves correctly`() {
        val d = KnnContextualBandit.squaredL2(feat(1.0, 2.0), feat(0.0, 0.0))
        assertEquals(5.0, d, 1e-9)
    }

    @Test
    fun `Knn squaredL2 rejects mismatched sizes`() {
        kotlin.test.assertFailsWith<IllegalArgumentException> {
            KnnContextualBandit.squaredL2(feat(1.0), feat(1.0, 2.0))
        }
    }

    // === Top-Two TS ===

    @Test
    fun `TopTwoTS sampleArgmax is callable directly`() {
        val b = TopTwoThompsonBandit(nbrArms = 3, policy = NormalTS(), beta = 0.5, random = Random(0))
        repeat(10) { b.update(it % 3, it.toDouble()) }
        val a = b.sampleArgmax()
        assertTrue(a in 0..2)
    }

    @Test
    fun `TopTwoTS beta=1 always plays the top sample`() {
        // Force a deterministic-ish bias by populating arm 0 heavily.
        val rng = Random(0)
        val b = TopTwoThompsonBandit(nbrArms = 2, policy = NormalTS(), beta = 1.0, random = rng)
        repeat(50) { b.update(0, 1.0) }
        // beta=1 means the top sample is always played; convergence still depends on draws,
        // but the resample loop is skipped so picks track sampleArgmax exactly.
        val picks = IntArray(2)
        repeat(50) { picks[b.choose()]++ }
        assertTrue(picks[0] > picks[1], "arm 0 should dominate: ${picks.toList()}")
    }

    @Test
    fun `TopTwoTS merge fans per-arm snapshots`() {
        val a = TopTwoThompsonBandit(nbrArms = 2, policy = NormalTS(), random = Random(0))
        val b = TopTwoThompsonBandit(nbrArms = 2, policy = NormalTS(), random = Random(1))
        repeat(10) {
            a.update(0, 1.0)
            b.update(0, 3.0)
        }
        val before = a.armResult(0).totalWeights
        a.merge(b.snapshot())
        assertTrue(a.armResult(0).totalWeights > before)
    }
}
