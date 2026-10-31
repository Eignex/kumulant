package com.eignex.kumulant.bandit

import com.eignex.kumulant.bandit.univariate.Exp3ArmResult
import com.eignex.kumulant.bandit.univariate.Exp3Bandit
import com.eignex.kumulant.bandit.univariate.Greedy
import com.eignex.kumulant.bandit.univariate.MultiArmedBandit
import com.eignex.kumulant.bandit.univariate.RouletteWheelBandit
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class BanditTuningTest {

    @Test
    fun `the default EXP3 gamma leaves room for the learned weights`() {
        for (k in listOf(2, 3, 5, 10, 100)) {
            val gamma = Exp3Bandit(nbrArms = k).gamma
            assertTrue(gamma < 1.0, "gamma saturated at $gamma for $k arms")
            assertTrue(gamma > 0.0, "gamma vanished at $gamma for $k arms")
        }
    }

    @Test
    fun `EXP3 concentrates on the winning arm under default tuning`() {
        val bandit = Exp3Bandit(nbrArms = 3, random = Random(1))
        repeat(500) {
            bandit.update(0, 1.0)
            bandit.update(1, 0.0)
            bandit.update(2, 0.0)
        }

        val p = bandit.playDistribution()

        assertTrue(p[0] > 0.5, "arm 0 always won yet its play probability is only ${p[0]}")
        assertTrue(p[0] > p[1] && p[0] > p[2], "play distribution ignored the weights: ${p.toList()}")
    }

    @Test
    fun `EXP3 falls back to uniform rather than NaN when every weight underflows`() {
        val bandit = Exp3Bandit(nbrArms = 2, eta = 1.0, gamma = 0.1, random = Random(5))

        bandit.update(0, -1e5)
        bandit.update(1, -1e5)

        val p = bandit.playDistribution()
        assertTrue(p.all { it.isFinite() }, "distribution went non-finite: ${p.toList()}")
        assertEquals(1.0, p.sum(), 1e-9, "distribution must still normalise")
    }

    @Test
    fun `EXP3 survives a NaN reward`() {
        val bandit = Exp3Bandit(nbrArms = 2, gamma = 0.1, random = Random(5))

        bandit.update(0, Double.NaN)

        val p = bandit.playDistribution()
        assertTrue(p.all { it.isFinite() }, "distribution went non-finite: ${p.toList()}")
        assertEquals(1.0, p.sum(), 1e-9)
    }

    @Test
    fun `EXP3 survives merging non-finite arm results`() {
        val bandit = Exp3Bandit(nbrArms = 2, gamma = 0.1, random = Random(5))

        bandit.merge(listOf(Exp3ArmResult(Double.NEGATIVE_INFINITY), Exp3ArmResult(Double.NaN)))

        val p = bandit.playDistribution()
        assertTrue(p.all { it.isFinite() }, "distribution went non-finite: ${p.toList()}")
        assertEquals(1.0, p.sum(), 1e-9)
    }

    @Test
    fun `a zero weight does not move a roulette wheel arm`() {
        val touched = RouletteWheelBandit(nbrArms = 2, random = Random(11))
        val untouched = RouletteWheelBandit(nbrArms = 2, random = Random(11))

        repeat(10) { touched.update(0, 5.0, 0.0) }

        assertEquals(untouched.evaluate(0), touched.evaluate(0), 1e-12)
        assertEquals(untouched.evaluate(1), touched.evaluate(1), 1e-12)
    }

    @Test
    fun `an out-of-bounds arm index is rejected with a message`() {
        val bandit = MultiArmedBandit(2, Greedy(), Random(1))

        assertFailsWith<IllegalArgumentException> { bandit.update(5, 1.0) }
        assertFailsWith<IllegalArgumentException> { RouletteWheelBandit(nbrArms = 2).update(-1, 1.0) }
    }
}
