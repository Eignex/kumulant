package com.eignex.kumulant.bandit

import com.eignex.koblas.core.F64DenseVector
import com.eignex.koblas.core.F64VectorLike
import com.eignex.kumulant.bandit.contextual.Exp4Bandit
import com.eignex.kumulant.bandit.contextual.Exp4Expert
import com.eignex.kumulant.bandit.univariate.Exp3ArmResult
import com.eignex.kumulant.bandit.univariate.Exp3Bandit
import com.eignex.kumulant.bandit.univariate.MultiArmedBandit
import com.eignex.kumulant.bandit.univariate.RouletteWheelBandit
import com.eignex.kumulant.bandit.univariate.UCB1Normal
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

// A policy that silently stops learning is worse than one that fails loudly, so these assert on
// what `choose` actually does rather than on the internal weights.
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
    fun `EXP4 concentrates on the better expert under default tuning`() {
        val alwaysFirst = Exp4Expert { _, k -> DoubleArray(k) { if (it == 0) 1.0 else 0.0 } }
        val alwaysSecond = Exp4Expert { _, k -> DoubleArray(k) { if (it == 1) 1.0 else 0.0 } }
        val bandit = Exp4Bandit(nbrArms = 2, experts = listOf(alwaysFirst, alwaysSecond), random = Random(3))
        val x: F64VectorLike = F64DenseVector.of(doubleArrayOf(1.0))

        repeat(500) {
            bandit.update(0, x, 1.0)
            bandit.update(1, x, 0.0)
        }

        val p = bandit.playDistribution(x)
        assertTrue(p[0] > 0.5, "the winning expert's arm is only played ${p[0]} of the time")
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
    fun `EXP3 survives merging all-zero arm results`() {
        val bandit = Exp3Bandit(nbrArms = 2, gamma = 0.1, random = Random(5))

        bandit.merge(listOf(Exp3ArmResult(0.0), Exp3ArmResult(0.0)))

        val p = bandit.playDistribution()
        assertTrue(p.all { it.isFinite() }, "distribution went non-finite: ${p.toList()}")
        assertEquals(1.0, p.sum(), 1e-9)
    }

    @Test
    fun `UCB1Normal scores finitely and prefers the best arm`() {
        val bandit = MultiArmedBandit(3, UCB1Normal(), Random(7))
        repeat(30) {
            bandit.update(0, 1.0)
            bandit.update(1, 2.0)
            bandit.update(2, 3.0)
        }

        val scores = (0 until 3).map { bandit.evaluate(it) }
        assertTrue(scores.all { it.isFinite() }, "scores were not finite: $scores")
        assertTrue(scores[2] > scores[0], "arm 2 has the best mean but scores $scores")

        val counts = IntArray(3)
        repeat(300) { counts[bandit.choose()]++ }
        assertTrue(counts[2] > counts[0], "arm 2 has the best mean but was chosen ${counts.toList()}")
    }

    @Test
    fun `UCB1Normal still explores the losing arm at two arms`() {
        // Rewards have to vary: this policy scales its bonus by the observed variance, so constant
        // rewards give a zero bonus legitimately and would not tell us anything about the log term.
        val bandit = MultiArmedBandit(2, UCB1Normal(), Random(7))
        repeat(50) { i ->
            bandit.update(0, if (i % 2 == 0) 0.0 else 2.0) // mean 1
            bandit.update(1, if (i % 2 == 0) 4.0 else 6.0) // mean 5
        }

        val scores = (0 until 2).map { bandit.evaluate(it) }
        assertTrue(scores.all { it.isFinite() }, "scores were not finite: $scores")
        assertTrue(scores[0] > 1.0, "arm 0 has mean ~1.0 but scores ${scores[0]}, so the bonus is missing")
        assertTrue(scores[1] > 5.0, "arm 1 has mean ~5.0 but scores ${scores[1]}, so the bonus is missing")
    }

    @Test
    fun `UCB1Normal reports zero variance rather than NaN for identical samples`() {
        val bandit = MultiArmedBandit(2, UCB1Normal(), Random(7))
        repeat(30) {
            bandit.update(0, 5.0)
            bandit.update(1, 5.0)
        }

        assertTrue(bandit.evaluate(0).isFinite(), "a zero-variance arm scored ${bandit.evaluate(0)}")
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
        val bandit = MultiArmedBandit(2, UCB1Normal(), Random(1))

        assertFailsWith<IllegalArgumentException> { bandit.update(5, 1.0) }
        assertFailsWith<IllegalArgumentException> { RouletteWheelBandit(nbrArms = 2).update(-1, 1.0) }
    }
}
