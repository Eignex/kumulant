package com.eignex.kumulant.bandit

import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class UnivariateBanditTest {

    @Test
    fun `MultiArmedBandit rejects non-positive nbrArms`() {
        assertFailsWith<IllegalArgumentException> {
            MultiArmedBandit(nbrArms = 0, policy = BetaBernoulliTS(), random = Random(0))
        }
        assertFailsWith<IllegalArgumentException> {
            MultiArmedBandit(nbrArms = -1, policy = BetaBernoulliTS(), random = Random(0))
        }
    }

    @Test
    fun `MultiArmedBandit choose returns a valid arm index`() {
        val mab = MultiArmedBandit(nbrArms = 5, policy = BetaBernoulliTS(), random = Random(7))
        repeat(20) {
            val i = mab.choose()
            assertTrue(i in 0..4)
        }
    }

    @Test
    fun `MultiArmedBandit choose does not mutate arm state`() {
        val mab = MultiArmedBandit(nbrArms = 3, policy = BetaBernoulliTS(), random = Random(0))
        mab.update(0, 1.0); mab.update(1, 0.0); mab.update(2, 1.0)
        val before = mab.snapshot()
        repeat(10) { mab.choose() }
        assertEquals(before, mab.snapshot())
    }

    @Test
    fun `MultiArmedBandit snapshot reflects per-arm updates`() {
        val mab = MultiArmedBandit(nbrArms = 2, policy = BetaBernoulliTS(1.0, 1.0), random = Random(0))
        mab.update(0, 1.0); mab.update(0, 1.0); mab.update(1, 0.0)
        val snap = mab.snapshot()
        assertEquals(2, snap.size)
        assertTrue(snap[0].successes > snap[1].successes)
        assertTrue(snap[0].trials > 2.0)
    }

    @Test
    fun `updateAll applies parallel arrays of indices and values`() {
        val mab = MultiArmedBandit(nbrArms = 2, policy = BetaBernoulliTS(), random = Random(0))
        mab.updateAll(intArrayOf(0, 1, 0, 1), doubleArrayOf(1.0, 0.0, 1.0, 0.0))
        val snap = mab.snapshot()
        assertTrue(snap[0].successes > snap[1].successes)
    }

    @Test
    fun `updateAll honors weights when provided`() {
        val a = MultiArmedBandit(nbrArms = 1, policy = BetaBernoulliTS(), random = Random(0))
        val b = MultiArmedBandit(nbrArms = 1, policy = BetaBernoulliTS(), random = Random(0))
        a.updateAll(intArrayOf(0, 0), doubleArrayOf(1.0, 0.0), doubleArrayOf(3.0, 1.0))
        b.updateAll(intArrayOf(0, 0), doubleArrayOf(1.0, 0.0))
        assertTrue(a.snapshot()[0].successes > b.snapshot()[0].successes)
    }

    @Test
    fun `updateAll rejects mismatched sizes`() {
        val mab = MultiArmedBandit(nbrArms = 2, policy = BetaBernoulliTS(), random = Random(0))
        assertFailsWith<IllegalArgumentException> {
            mab.updateAll(intArrayOf(0, 1), doubleArrayOf(1.0))
        }
        assertFailsWith<IllegalArgumentException> {
            mab.updateAll(intArrayOf(0), doubleArrayOf(1.0), doubleArrayOf(1.0, 1.0))
        }
    }

    @Test
    fun `RouletteWheelBandit rejects invalid configuration`() {
        assertFailsWith<IllegalArgumentException> { RouletteWheelBandit(nbrArms = 0) }
        assertFailsWith<IllegalArgumentException> { RouletteWheelBandit(nbrArms = 1, reactionFactor = -0.1) }
        assertFailsWith<IllegalArgumentException> { RouletteWheelBandit(nbrArms = 1, reactionFactor = 1.5) }
        assertFailsWith<IllegalArgumentException> { RouletteWheelBandit(nbrArms = 1, segmentLength = 0) }
        assertFailsWith<IllegalArgumentException> { RouletteWheelBandit(nbrArms = 1, minWeight = 0.0) }
    }

    @Test
    fun `RouletteWheelBandit choose returns valid index`() {
        val bandit = RouletteWheelBandit(nbrArms = 4, random = Random(1))
        repeat(50) {
            val i = bandit.choose()
            assertTrue(i in 0..3)
        }
    }

    @Test
    fun `RouletteWheelBandit choose falls back to uniform when all weights are at minWeight zeroish`() {
        val bandit = RouletteWheelBandit(
            nbrArms = 3,
            reactionFactor = 1.0,
            segmentLength = 1,
            initialWeight = 1.0,
            minWeight = 1e-300,
            random = Random(2),
        )
        repeat(20) { bandit.update(0, 0.0) }
        repeat(20) {
            val i = bandit.choose()
            assertTrue(i in 0..2)
        }
    }

    @Test
    fun `RouletteWheelBandit update accepts custom weight argument`() {
        val bandit = RouletteWheelBandit(
            nbrArms = 1,
            reactionFactor = 1.0,
            segmentLength = 2,
            initialWeight = 1.0,
            minWeight = 0.001,
            random = Random(3),
        )
        bandit.update(0, 2.0, weight = 3.0)
        bandit.update(0, 0.0, weight = 1.0)
        assertEquals(3.0, bandit.snapshot()[0].weight)
    }

    @Test
    fun `RouletteWheelBandit rebalance leaves arms with no calls at their current weight`() {
        val bandit = RouletteWheelBandit(
            nbrArms = 3,
            reactionFactor = 0.5,
            segmentLength = 2,
            initialWeight = 1.0,
            random = Random(0),
        )
        bandit.update(0, 5.0); bandit.update(0, 5.0)
        val snap = bandit.snapshot()
        assertTrue(snap[0].weight > 1.0)
        assertEquals(1.0, snap[1].weight)
        assertEquals(1.0, snap[2].weight)
    }
}
