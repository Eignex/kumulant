package com.eignex.kumulant.bandit

import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Behavioural checks for [MultiArmedBandit.evaluate]: it should be a read-only
 * scoring op (no step increment, no policy state mutation) and agree with the
 * value the policy would have used for that arm inside [choose].
 */
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
        // evaluate is a read-only scoring op: it consumes the RNG but never touches
        // policy/arm sufficient statistics. Confirm via snapshot equality before/after.
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
        assertTrue(kotlin.math.abs(mean - 2.0 / 7.0) < 0.02, "mean=$mean")
    }
}
