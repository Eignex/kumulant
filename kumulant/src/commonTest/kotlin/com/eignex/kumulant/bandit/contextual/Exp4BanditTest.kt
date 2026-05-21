package com.eignex.kumulant.bandit.contextual

import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.math.VectorView
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
class Exp4BanditTest {

    private fun feat(vararg xs: Double): DenseVector = DenseVector.of(xs)

    @Test
    fun `Exp4Bandit rejects bad inputs`() {
        assertFailsWith<IllegalArgumentException> {
            Exp4Bandit(nbrArms = 0, experts = listOf(uniformExpert(2)))
        }
        assertFailsWith<IllegalArgumentException> {
            Exp4Bandit(nbrArms = 2, experts = emptyList())
        }
    }

    @Test
    fun `playDistribution is uniform with one uniform expert`() {
        val bandit = Exp4Bandit(nbrArms = 3, experts = listOf(uniformExpert(3)), random = Random(0))
        val p = bandit.playDistribution(feat(0.0, 0.0))
        for (a in 0 until 3) assertTrue(abs(p[a] - 1.0 / 3) < 1e-9, "p[$a]=${p[a]}")
    }

    @Test
    fun `weights shift toward the best-matching expert`() {
        // Two experts: A always picks arm 0; B always picks arm 1. Arm 1 always rewards 1, arm 0 rewards 0.
        val expertA = Exp4Expert { _, n -> DoubleArray(n).also { it[0] = 1.0 } }
        val expertB = Exp4Expert { _, n -> DoubleArray(n).also { it[1] = 1.0 } }
        val rng = Random(1)
        val bandit = Exp4Bandit(
            nbrArms = 2,
            experts = listOf(expertA, expertB),
            eta = 0.5,
            gamma = 0.1,
            random = rng,
        )
        repeat(200) {
            val x = feat(0.0)
            val arm = bandit.choose(x)
            val reward = if (arm == 1) 1.0 else 0.0
            bandit.update(arm, x, reward)
        }
        val w = bandit.expertWeights()
        assertTrue(w[1] > w[0], "expert B should dominate: $w")
    }

    @Test
    fun `reset returns weights to uniform`() {
        val bandit = Exp4Bandit(
            nbrArms = 2,
            experts = listOf(uniformExpert(2), oneHotExpert(2, 0)),
            random = Random(2),
        )
        repeat(50) { bandit.update(0, feat(0.0), 1.0) }
        bandit.reset()
        val w = bandit.expertWeights()
        assertEquals(0.5, w[0], 1e-9)
        assertEquals(0.5, w[1], 1e-9)
    }

    @Test
    fun `create spawns independent replica`() {
        val a = Exp4Bandit(nbrArms = 2, experts = listOf(uniformExpert(2)), random = Random(3))
        val b = a.create(Random(4))
        repeat(20) { a.update(0, feat(0.0), 1.0) }
        // a's only expert is still alone; its weight ratio doesn't move from 1, but b is untouched.
        val wb = b.expertWeights()
        assertEquals(1.0, wb[0], 1e-9)
    }

    private fun uniformExpert(nbrArms: Int) = Exp4Expert { _, n ->
        DoubleArray(n) { 1.0 / n }.also { require(n == nbrArms) }
    }

    private fun oneHotExpert(nbrArms: Int, arm: Int) = Exp4Expert { _, n ->
        DoubleArray(n).also {
            it[arm] = 1.0
            require(n == nbrArms)
        }
    }

    @Test
    fun `context-aware experts win on context-dependent rewards`() {
        // Expert "left" picks arm 0 when x<0, arm 1 when x>=0; opposite for "right".
        val left = Exp4Expert { x: VectorView, n: Int ->
            DoubleArray(n).also { it[if (x[0] < 0.0) 0 else 1] = 1.0 }
        }
        val right = Exp4Expert { x: VectorView, n: Int ->
            DoubleArray(n).also { it[if (x[0] < 0.0) 1 else 0] = 1.0 }
        }
        val rng = Random(5)
        val bandit = Exp4Bandit(
            nbrArms = 2,
            experts = listOf(left, right),
            eta = 0.5,
            gamma = 0.1,
            random = rng,
        )
        // True reward: matches "left"'s policy.
        repeat(400) {
            val x = feat(rng.nextDouble() * 2 - 1)
            val a = bandit.choose(x)
            val best = if (x[0] < 0.0) 0 else 1
            val reward = if (a == best) 1.0 else 0.0
            bandit.update(a, x, reward)
        }
        val w = bandit.expertWeights()
        assertTrue(w[0] > w[1], "left expert should dominate: $w")
    }
}
