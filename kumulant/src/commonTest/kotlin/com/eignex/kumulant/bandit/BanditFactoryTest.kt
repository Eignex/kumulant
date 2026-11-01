package com.eignex.kumulant.bandit

import com.eignex.kumulant.bandit.univariate.BernoulliArm
import com.eignex.kumulant.bandit.univariate.BetaPosterior
import com.eignex.kumulant.bandit.univariate.BoltzmannSpec
import com.eignex.kumulant.bandit.univariate.Exp3Bandit
import com.eignex.kumulant.bandit.univariate.Exp3Spec
import com.eignex.kumulant.bandit.univariate.MultiArmedBandit
import com.eignex.kumulant.bandit.univariate.MultiArmedSpec
import com.eignex.kumulant.bandit.univariate.NormalArm
import com.eignex.kumulant.bandit.univariate.NormalGammaPosterior
import com.eignex.kumulant.bandit.univariate.RouletteWheelSpec
import com.eignex.kumulant.bandit.univariate.ThompsonSamplingSpec
import com.eignex.kumulant.bandit.univariate.TopTwoThompsonSpec
import com.eignex.kumulant.bandit.univariate.UnivariateBanditSpec
import com.eignex.kumulant.core.Concurrency
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class BanditFactoryTest {

    @Test
    fun `MultiArmed spec materialises to a working MultiArmedBandit`() {
        val spec = MultiArmedSpec(
            nbrArms = 3,
            policy = ThompsonSamplingSpec(BernoulliArm(), BetaPosterior),
        )
        val b = (spec as UnivariateBanditSpec).materialize(Random(0))
        assertTrue(b is MultiArmedBandit<*>)
        assertEquals(3, b.nbrArms)
    }

    @Test
    fun `a MultiArmed spec builds its arm stats with the requested concurrency`() {
        val spec = MultiArmedSpec(nbrArms = 2, policy = ThompsonSamplingSpec(NormalArm(), NormalGammaPosterior))

        val b = (spec as UnivariateBanditSpec).materialize(Random(0), Concurrency.Strict)

        assertEquals(Concurrency.Strict, (b as MultiArmedBandit<*>).armStat(0).concurrency)
    }

    @Test
    fun `RouletteWheel spec materialises`() {
        val b = RouletteWheelSpec(nbrArms = 4, segmentLength = 5).materialize(Random(0))
        assertEquals(4, b.nbrArms)
    }

    @Test
    fun `Boltzmann spec materialises as a multi-armed policy`() {
        val b = MultiArmedSpec(nbrArms = 3, policy = BoltzmannSpec()).materialize(Random(0))
        assertEquals(3, b.nbrArms)
    }

    @Test
    fun `a spec-built Exp3 resolves the same gamma as the constructor`() {
        for (arms in 2..12) {
            val fromSpec = Exp3Spec(nbrArms = arms).materialize(Random(0))
            val direct = Exp3Bandit(nbrArms = arms, random = Random(0))
            assertEquals(direct.gamma, fromSpec.gamma, "gamma disagrees at K=$arms")
            assertTrue(fromSpec.gamma < 1.0, "gamma saturated at K=$arms, so the play distribution is uniform")
        }
    }

    @Test
    fun `a spec-built Exp3 lets its weights steer the play distribution`() {
        val b = Exp3Spec(nbrArms = 3).materialize(Random(0))
        repeat(50) { b.update(armIndex = 0, value = 1.0) }

        val p = b.playDistribution()
        assertTrue(p[0] > p[1], "the rewarded arm should be played more often: ${p.toList()}")
    }

    @Test
    fun `TopTwoThompson spec materialises`() {
        val b = TopTwoThompsonSpec(
            nbrArms = 3,
            policy = ThompsonSamplingSpec(NormalArm(), NormalGammaPosterior),
        ).materialize(Random(0))
        assertEquals(3, b.nbrArms)
    }
}
