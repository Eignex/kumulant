package com.eignex.kumulant.bandit.univariate

import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
class Exp3BanditTest {

    @Test
    fun `rejects bad inputs`() {
        assertFailsWith<IllegalArgumentException> { Exp3Bandit(nbrArms = 0) }
        assertFailsWith<IllegalArgumentException> { Exp3Bandit(nbrArms = 2, eta = -0.1) }
        assertFailsWith<IllegalArgumentException> { Exp3Bandit(nbrArms = 2, gamma = 1.5) }
    }

    @Test
    fun `playDistribution starts uniform`() {
        val b = Exp3Bandit(nbrArms = 3, random = Random(0))
        val p = b.playDistribution()
        for (a in 0 until 3) assertTrue(abs(p[a] - 1.0 / 3) < 1e-9)
    }

    @Test
    fun `weights shift toward the best arm`() {
        val rng = Random(1)
        val b = Exp3Bandit(nbrArms = 3, eta = 0.3, gamma = 0.1, random = rng)
        repeat(400) {
            val a = b.choose()
            val reward = if (a == 1) 1.0 else 0.0
            b.update(a, reward)
        }
        val w = b.armWeights()
        assertTrue(w[1] > w[0] && w[1] > w[2], "arm 1 should dominate: ${w.toList()}")
    }

    @Test
    fun `reset returns weights to uniform`() {
        val b = Exp3Bandit(nbrArms = 2, random = Random(2))
        repeat(50) { b.update(0, 1.0) }
        b.reset()
        val w = b.armWeights()
        assertEquals(0.5, w[0], 1e-9)
        assertEquals(0.5, w[1], 1e-9)
    }

    @Test
    fun `feedback order does not change the weight an arm earns`() {
        fun weightsAfter(delayed: Boolean): List<Double> {
            val bandit = Exp3Bandit(nbrArms = 2, random = Random(1))
            bandit.choose()
            bandit.choose()
            if (delayed) {
                bandit.update(0, 1.0)
                bandit.update(1, 1.0)
            } else {
                bandit.update(1, 1.0)
                bandit.update(0, 1.0)
            }
            return bandit.armWeights().toList()
        }
        val direct = weightsAfter(delayed = false)
        val delayed = weightsAfter(delayed = true)
        for (i in direct.indices) assertTrue(abs(direct[i] - delayed[i]) < 1e-9, "arm $i: $direct vs $delayed")
    }
}

class Exp3InertWeightTest {

    @Test
    fun `a zero-weight update does not consume a recorded propensity`() {
        fun run(withInertUpdate: Boolean): DoubleArray {
            val b = Exp3Bandit(nbrArms = 2, eta = 0.5, gamma = 0.1, random = Random(1))
            b.choose()
            b.choose()
            if (withInertUpdate) b.update(0, 1.0, 0.0)
            b.update(1, 1.0, 1.0)
            b.update(0, 1.0, 1.0)
            return b.armWeights()
        }
        assertContentEquals(run(withInertUpdate = false).toList(), run(withInertUpdate = true).toList())
    }
}
