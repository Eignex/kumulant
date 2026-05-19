package com.eignex.kumulant.bandit

import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class TopTwoThompsonBanditTest {

    @Test
    fun `rejects bad inputs`() {
        assertFailsWith<IllegalArgumentException> {
            TopTwoThompsonBandit(nbrArms = 1, policy = NormalTS())
        }
        assertFailsWith<IllegalArgumentException> {
            TopTwoThompsonBandit(nbrArms = 2, policy = NormalTS(), beta = -0.1)
        }
        assertFailsWith<IllegalArgumentException> {
            TopTwoThompsonBandit(nbrArms = 2, policy = NormalTS(), maxResamples = 0)
        }
    }

    @Test
    fun `top-two converges to the best arm under noisy rewards`() {
        val rng = Random(1)
        val b = TopTwoThompsonBandit(
            nbrArms = 3,
            policy = NormalTS(),
            beta = 0.5,
            random = rng,
        )
        // arm 0 is best; rewards are noisy enough that the runner-up keeps getting play during training.
        repeat(600) {
            val a = b.choose()
            val mean = when (a) { 0 -> 1.0
                1 -> 0.5
                else -> 0.0 }
            b.update(a, mean + rng.nextDouble() * 0.5 - 0.25)
        }
        val picks = IntArray(3)
        repeat(300) { picks[b.choose()]++ }
        assertTrue(picks[0] > picks[1] && picks[0] > picks[2], "arm 0 should dominate: ${picks.toList()}")
    }

    @Test
    fun `top-two spreads play across the top two arms during training`() {
        // Use a fresh bandit and never let it converge — every choose is at the prior +
        // a handful of observations, so resamples land on different arms often.
        val rng = Random(2)
        val b = TopTwoThompsonBandit(nbrArms = 2, policy = NormalTS(), beta = 0.5, random = rng)
        val picks = IntArray(2)
        repeat(200) { picks[b.choose()]++ }
        assertTrue(picks[0] > 30 && picks[1] > 30, "both arms should get explored: ${picks.toList()}")
    }

    @Test
    fun `reset restores priors`() {
        val b = TopTwoThompsonBandit(nbrArms = 2, policy = NormalTS(), random = Random(2))
        repeat(50) { b.update(0, 1.0) }
        b.reset()
        val r0 = b.armResult(0)
        // After reset the per-arm running mean is back at the prior.
        assertTrue(r0.totalWeights < 1.0, "expected reset to clear data, got totalWeights=${r0.totalWeights}")
    }
}
