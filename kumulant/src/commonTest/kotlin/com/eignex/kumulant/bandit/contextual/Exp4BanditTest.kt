package com.eignex.kumulant.bandit.contextual

import com.eignex.koblas.core.F64VectorLike
import com.eignex.kumulant.feat
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class Exp4BanditTest {

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
    fun `playDistributionInto agrees with owned distribution and keeps its destination`() {
        val bandit = Exp4Bandit(
            nbrArms = 3,
            experts = listOf(oneHotExpert(3, 0), oneHotExpert(3, 2)),
            gamma = 0.2,
        )
        val out = DoubleArray(3)

        bandit.playDistributionInto(feat(0.0), out)
        val owned = bandit.playDistribution(feat(0.0))

        assertEquals(owned.toList(), out.toList())
        assertTrue(out !== owned)
    }

    @Test
    fun `playDistributionInto validates its destination before consulting experts`() {
        var calls = 0
        val bandit = Exp4Bandit(
            nbrArms = 2,
            experts = listOf(
                Exp4Expert { _, _ ->
                    calls++
                    doubleArrayOf(0.5, 0.5)
                },
            ),
        )

        assertFailsWith<IllegalArgumentException> { bandit.playDistributionInto(feat(0.0), DoubleArray(1)) }

        assertEquals(0, calls)
    }

    @Test
    fun `playDistributionInto evaluates experts in order once and preserves shared advice`() {
        val shared = doubleArrayOf(1.0, 0.0)
        val calls = mutableListOf<Int>()
        val experts = listOf(
            Exp4Expert { _, _ ->
                calls += 0
                shared[0] = 1.0
                shared[1] = 0.0
                shared
            },
            Exp4Expert { _, _ ->
                calls += 1
                shared[0] = 0.0
                shared[1] = 1.0
                shared
            },
        )
        val bandit = Exp4Bandit(nbrArms = 2, experts = experts, gamma = 0.0)
        val out = DoubleArray(2)

        bandit.playDistributionInto(feat(0.0), out)

        assertEquals(listOf(0, 1), calls)
        assertEquals(listOf(0.0, 1.0), out.toList())
    }

    @Test
    fun `playDistributionInto preserves exceptional arithmetic`() {
        val advice = arrayOf(
            doubleArrayOf(Double.POSITIVE_INFINITY, -0.0),
            doubleArrayOf(0.0, Double.NaN),
        )
        val allocating = Exp4Bandit(2, advice.map { values -> Exp4Expert { _, _ -> values } }, gamma = 1.0)
        val destination = Exp4Bandit(2, advice.map { values -> Exp4Expert { _, _ -> values } }, gamma = 1.0)
        val out = DoubleArray(2)

        val expected = allocating.playDistribution(feat(0.0))
        destination.playDistributionInto(feat(0.0), out)

        for (i in out.indices) assertEquals(expected[i], out[i])
    }

    @Test
    fun `choose consumes one draw and matches destination distribution away from a boundary`() {
        val experts = listOf(oneHotExpert(2, 0), oneHotExpert(2, 0), oneHotExpert(2, 1))
        val expected = Exp4Bandit(2, experts, gamma = 0.0, random = Random(17))
        val actual = Exp4Bandit(2, experts, gamma = 0.0, random = Random(17))
        val out = DoubleArray(2)

        expected.playDistributionInto(feat(0.0), out)
        val arm = actual.choose(feat(0.0))

        assertEquals(if (Random(17).nextDouble() < out[0]) 0 else 1, arm)
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
        val left = Exp4Expert { x: F64VectorLike, n: Int ->
            DoubleArray(n).also { it[if (x[0] < 0.0) 0 else 1] = 1.0 }
        }
        val right = Exp4Expert { x: F64VectorLike, n: Int ->
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

    @Test
    fun `zero-weight update leaves the expert weights untouched`() {
        val expertA = Exp4Expert { _, n -> DoubleArray(n).also { it[0] = 1.0 } }
        val expertB = Exp4Expert { _, n -> DoubleArray(n).also { it[1] = 1.0 } }
        val bandit = Exp4Bandit(nbrArms = 2, experts = listOf(expertA, expertB), random = Random(1))
        val before = bandit.expertWeights().toList()
        bandit.update(0, feat(0.0), 1.0, weight = 0.0)
        assertEquals(before, bandit.expertWeights().toList())
    }

    @Test
    fun `inert update does not consume a pending propensity`() {
        val bandit = Exp4Bandit(nbrArms = 2, experts = listOf(oneHotExpert(2, 0), oneHotExpert(2, 1)), gamma = 0.2)
        val x = feat(0.0)
        val arm = bandit.choose(x)

        bandit.update(arm, x, 1.0, weight = 0.0)
        bandit.update(arm, x, 1.0)

        assertTrue(bandit.expertWeights()[arm] > 0.5)
    }

    @Test
    fun `observation weight scales the expert weight update`() {
        fun weightsAfter(observationWeight: Double): List<Double> {
            val expertA = Exp4Expert { _, n -> DoubleArray(n).also { it[0] = 1.0 } }
            val expertB = Exp4Expert { _, n -> DoubleArray(n).also { it[1] = 1.0 } }
            val bandit = Exp4Bandit(nbrArms = 2, experts = listOf(expertA, expertB), random = Random(1))
            bandit.update(0, feat(0.0), 1.0, weight = observationWeight)
            return bandit.expertWeights().toList()
        }
        assertTrue(weightsAfter(5.0)[0] > weightsAfter(1.0)[0])
    }
}
