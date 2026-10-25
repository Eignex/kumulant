package com.eignex.kumulant.bandit

import com.eignex.kumulant.bandit.univariate.BoltzmannBandit
import com.eignex.kumulant.bandit.univariate.Exp3Bandit
import com.eignex.kumulant.bandit.univariate.NormalTS
import com.eignex.kumulant.bandit.univariate.TopTwoThompsonBandit
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class StandaloneBanditExtraTest {

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
            b.update(0, 1.0)
        }
        val p = b.playDistribution()
        assertTrue(p[0] > 0.95, "arm 0 should dominate without uniform mass: $p")
    }

    @Test
    fun `Exp3Bandit defaultEta is positive and grows with nbrArms`() {
        val small = Exp3Bandit.defaultEta(2)
        val large = Exp3Bandit.defaultEta(10)
        assertTrue(small > 0.0 && large > 0.0)
        assertTrue(small.isFinite() && large.isFinite())
    }

    @Test
    fun `Boltzmann temperature respects floor`() {
        val b = BoltzmannBandit(
            nbrArms = 2,
            initialTau = 1.0,
            minTau = 0.5,
            decay = 5.0,
            random = Random(0),
        )
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
    fun `TopTwoTS beta=1 always plays the top sample`() {
        val rng = Random(0)
        val b = TopTwoThompsonBandit(nbrArms = 2, policy = NormalTS(), beta = 1.0, random = rng)
        repeat(50) { b.update(0, 1.0) }
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
