package com.eignex.kumulant.bandit

import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class NewerUcbPoliciesTest {

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
            val reward = when (a) { 0 -> 0.1
                1 -> 0.7
                else -> 0.3 } + rng.nextDouble() * 0.05
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
            val mean = when (a) { 0 -> 0.2
                1 -> 0.8
                else -> 0.5 }
            bandit.update(a, mean + rng.nextDouble() * 0.1)
        }
        val picks = IntArray(3)
        repeat(300) { picks[bandit.choose()]++ }
        assertTrue(picks[1] > picks[0] && picks[1] > picks[2], "arm 1 should dominate: ${picks.toList()}")
    }
}
