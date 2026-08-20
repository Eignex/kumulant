package com.eignex.kumulant.bandit.univariate

import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RouletteWheelBanditTest {

    @Test
    fun `weights stay at initial value before segment rebalance fires`() {
        val bandit = RouletteWheelBandit(
            nbrArms = 3,
            reactionFactor = 0.5,
            segmentLength = 4,
            initialWeight = 1.0,
            random = Random(1),
        )
        val before = bandit.snapshot().map { it.weight }
        // 3 updates < segmentLength=4 -> no rebalance yet.
        bandit.update(0, 3.0)
        bandit.update(1, 0.0)
        bandit.update(0, 3.0)
        assertEquals(before, bandit.snapshot().map { it.weight })
    }

    @Test
    fun `segment rebalance raises high-reward arm`() {
        val bandit = RouletteWheelBandit(
            nbrArms = 2,
            reactionFactor = 0.5,
            segmentLength = 4,
            initialWeight = 1.0,
            random = Random(1),
        )
        // 4 updates: arm 0 gets reward 3.0 twice, arm 1 gets 0.0 twice.
        bandit.update(0, 3.0)
        bandit.update(1, 0.0)
        bandit.update(0, 3.0)
        bandit.update(1, 0.0)
        val after = bandit.snapshot()
        // arm 0: w = 1*0.5 + 0.5*3 = 2.0
        // arm 1: w = 1*0.5 + 0.5*0 = 0.5
        assertEquals(2.0, after[0].weight)
        assertEquals(0.5, after[1].weight)
    }

    @Test
    fun `min weight floor prevents arm extinction`() {
        val bandit = RouletteWheelBandit(
            nbrArms = 2,
            reactionFactor = 1.0,
            segmentLength = 1,
            initialWeight = 1.0,
            minWeight = 0.5,
            random = Random(1),
        )
        // Arm 0 gets repeatedly bad reward; weight should not drop below 0.5.
        repeat(20) { bandit.update(0, 0.0) }
        assertTrue(bandit.snapshot()[0].weight >= 0.5)
    }

    @Test
    fun `evaluate returns current weight without mutation`() {
        val bandit = RouletteWheelBandit(nbrArms = 3, random = Random(0))
        val w0 = bandit.evaluate(0)
        assertEquals(1.0, w0)
        // Read does not affect any internal counter.
        bandit.evaluate(1)
        bandit.evaluate(2)
        bandit.evaluate(0)
        assertEquals(1.0, bandit.evaluate(0))
    }

    @Test
    fun `segment rebalance is invariant to a uniform observation weight`() {
        fun weightAfter(observationWeight: Double): Double {
            val bandit = RouletteWheelBandit(
                nbrArms = 2,
                reactionFactor = 0.5,
                segmentLength = 2,
                initialWeight = 1.0,
                random = Random(1),
            )
            bandit.update(0, 1.0, observationWeight)
            bandit.update(0, 1.0, observationWeight)
            return bandit.snapshot()[0].weight
        }
        assertEquals(weightAfter(1.0), weightAfter(3.0))
    }
}

class RouletteWheelNonFiniteRewardTest {

    @Test
    fun `a NaN reward does not pin choose to the last arm`() {
        val b = RouletteWheelBandit(nbrArms = 3, segmentLength = 10, random = Random(0))
        repeat(9) { b.update(it % 3, 1.0) }
        b.update(0, Double.NaN)
        val picks = (0 until 200).map { b.choose() }.toSet()
        assertTrue(picks.size > 1, "every choose returned ${picks.first()}")
    }

    @Test
    fun `a NaN reward does not leave an arm weight non-finite`() {
        val b = RouletteWheelBandit(nbrArms = 3, segmentLength = 10, random = Random(0))
        repeat(9) { b.update(it % 3, 1.0) }
        b.update(0, Double.NaN)
        assertTrue(b.snapshot().all { it.weight.isFinite() }, "weights=${b.snapshot()}")
    }
}
