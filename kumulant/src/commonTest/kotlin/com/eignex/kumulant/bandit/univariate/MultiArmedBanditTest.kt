package com.eignex.kumulant.bandit.univariate

import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class MultiArmedBanditTest {

    @Test
    fun `evaluate returns a finite score for each arm`() {
        val mab = MultiArmedBandit(nbrArms = 4, policy = BetaBernoulliTS(), random = Random(1))
        mab.update(0, 1.0)
        mab.update(1, 0.0)
        mab.update(2, 1.0)
        mab.update(3, 0.0)
        for (i in 0 until 4) {
            val s = mab.evaluate(i)
            assertTrue(s.isFinite(), "arm $i score=$s not finite")
        }
    }

    @Test
    fun `evaluate does not mutate arm state`() {
        val mab = MultiArmedBandit(nbrArms = 3, policy = BetaBernoulliTS(), random = Random(42))
        mab.update(0, 1.0)
        mab.update(1, 1.0)
        mab.update(2, 0.0)
        val before = mab.snapshot()
        repeat(50) { i -> mab.evaluate(i % 3) }
        val after = mab.snapshot()
        assertEquals(before, after)
    }

    @Test
    fun `evaluate matches policy evaluate for the same snapshot`() {
        // With a seeded RNG, evaluating twice in a row should produce different draws
        // (Thompson sampling consumes the RNG), but the *distribution* is the same one
        // the policy would have used in choose(). Spot-check by comparing means.
        val rng = Random(7)
        val mab = MultiArmedBandit(nbrArms = 1, policy = BetaBernoulliTS(2.0, 5.0), random = rng)
        var sum = 0.0
        val n = 5000
        repeat(n) { sum += mab.evaluate(0) }
        val mean = sum / n
        // Beta(2,5) mean is 2/7 ~= 0.2857.
        assertTrue(abs(mean - 2.0 / 7.0) < 0.02, "mean=$mean")
    }

    @Test
    fun `armResult matches snapshot at index`() {
        val mab = MultiArmedBandit(nbrArms = 3, policy = BetaBernoulliTS(), random = Random(0))
        mab.update(0, 1.0)
        mab.update(1, 1.0)
        mab.update(2, 0.0)
        val snap = mab.snapshot()
        for (i in 0 until 3) assertEquals(snap[i], mab.armResult(i))
    }

    @Test
    fun `armStat exposes the live per-arm SeriesStat`() {
        val mab = MultiArmedBandit(nbrArms = 2, policy = BetaBernoulliTS(), random = Random(0))
        mab.update(0, 1.0)
        val stat = mab.armStat(0)
        assertEquals(mab.armResult(0), stat.read(0L))
    }

    @Test
    fun `merge per-arm folds another bandit's snapshot through the policy`() {
        val a = MultiArmedBandit(nbrArms = 2, policy = BetaBernoulliTS(), random = Random(1))
        val b = MultiArmedBandit(nbrArms = 2, policy = BetaBernoulliTS(), random = Random(2))
        repeat(40) {
            a.update(0, 1.0)
            a.update(1, 0.0)
        }
        repeat(40) {
            b.update(0, 1.0)
            b.update(1, 0.0)
        }
        val before = a.armResult(0).trials
        a.merge(b.snapshot())
        val after = a.armResult(0).trials
        assertTrue(after > before, "merged trials should exceed pre-merge: $before -> $after")
    }

    @Test
    fun `merge rejects nbrArms mismatch`() {
        val mab = MultiArmedBandit(nbrArms = 2, policy = BetaBernoulliTS())
        val wrongSize = MultiArmedBandit(nbrArms = 3, policy = BetaBernoulliTS()).snapshot()
        assertFailsWith<IllegalArgumentException> { mab.merge(wrongSize) }
    }

    @Test
    fun `reset rebuilds per-arm stats to prior baseline`() {
        val mab = MultiArmedBandit(nbrArms = 2, policy = BetaBernoulliTS(2.0, 5.0), random = Random(0))
        repeat(50) { mab.update(0, 1.0) }
        val before = mab.armResult(0).trials
        mab.reset()
        val after = mab.armResult(0).trials
        // After reset, only the prior pseudo-counts (2 + 5 = 7) remain.
        assertEquals(7.0, after, 1e-9)
        assertTrue(before > after, "reset shrank trials: $before -> $after")
    }

    @Test
    fun `create returns a fresh bandit with the same configuration`() {
        val original = MultiArmedBandit(nbrArms = 3, policy = BetaBernoulliTS(), random = Random(0))
        repeat(30) { original.update(0, 1.0) }
        val fresh = original.create(original.random)
        assertEquals(3, fresh.snapshot().size)
        // Fresh has only prior pseudo-counts on each arm
        val freshTrials = fresh.armResult(0).trials
        val origTrials = original.armResult(0).trials
        assertTrue(origTrials > freshTrials)
    }
}
