package com.eignex.kumulant.bandit.univariate

import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class BoltzmannBanditTest {

    @Test
    fun `rejects bad inputs`() {
        assertFailsWith<IllegalArgumentException> { BoltzmannBandit(nbrArms = 0) }
        assertFailsWith<IllegalArgumentException> { BoltzmannBandit(nbrArms = 2, initialTau = 0.0) }
        assertFailsWith<IllegalArgumentException> { BoltzmannBandit(nbrArms = 2, minTau = 0.0) }
        assertFailsWith<IllegalArgumentException> { BoltzmannBandit(nbrArms = 2, decay = -1.0) }
    }

    @Test
    fun `softmax concentrates on the best arm as temperature cools`() {
        val rng = Random(1)
        val b = BoltzmannBandit(
            nbrArms = 3,
            initialTau = 1.0,
            minTau = 1e-3,
            decay = 0.5,
            random = rng,
        )
        repeat(500) {
            val a = b.choose()
            val reward = when (a) { 0 -> 0.0
                1 -> 1.0
                else -> 0.5 }
            b.update(a, reward)
        }
        val picks = IntArray(3)
        repeat(500) { picks[b.choose()]++ }
        assertTrue(picks[1] > picks[0] && picks[1] > picks[2], "arm 1 should dominate: ${picks.toList()}")
    }

    @Test
    fun `temperature follows the cooling schedule`() {
        val b = BoltzmannBandit(nbrArms = 2, initialTau = 1.0, decay = 1.0, random = Random(2))
        b.playDistribution()
        b.playDistribution()
        b.playDistribution()
        // step=3, tau = 1/3
        assertTrue(kotlin.math.abs(b.temperature() - 1.0 / 3.0) < 1e-9)
    }

    @Test
    fun `reset restores priors and step counter`() {
        val b = BoltzmannBandit(nbrArms = 2, initialTau = 1.0, decay = 1.0, random = Random(3))
        repeat(20) { b.update(0, 1.0) }
        b.choose()
        b.reset()
        // After reset, temperature returns to initial after the first call.
        b.playDistribution()
        assertTrue(kotlin.math.abs(b.temperature() - 1.0) < 1e-9)
    }
}
