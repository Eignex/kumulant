package com.eignex.kumulant.bandit

import com.eignex.kumulant.bandit.contextual.RegressionContextualBandit
import com.eignex.kumulant.bandit.univariate.BetaBernoulliTS
import com.eignex.kumulant.bandit.univariate.MultiArmedBandit
import com.eignex.kumulant.bandit.univariate.NormalTS
import com.eignex.kumulant.bandit.univariate.RouletteWheelArmResult
import com.eignex.kumulant.bandit.univariate.RouletteWheelBandit
import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.stat.regression.BayesianRegressionStat
import com.eignex.kumulant.stat.regression.MultivariateGaussian
import com.eignex.kumulant.stat.summary.BernoulliSumResult
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * Polymorphic checks against the joint [Bandit] interface plus the [PerArmBandit]
 * convenience — anything that targets the per-arm state surface should work
 * uniformly across univariate and contextual flavours.
 */
class BanditInterfaceTest {

    @Test
    fun `MultiArmedBandit and RegressionContextualBandit both satisfy Bandit`() {
        val mab: Bandit = MultiArmedBandit(
            nbrArms = 3,
            policy = BetaBernoulliTS(),
            random = Random(0),
        )
        val cb: Bandit = RegressionContextualBandit(
            nbrArms = 3,
            template = BayesianRegressionStat(featureSize = 2),
            posterior = MultivariateGaussian,
            random = Random(0),
        )
        assertEquals(3, mab.nbrArms)
        assertEquals(3, cb.nbrArms)
    }

    @Test
    fun `RouletteWheelBandit also satisfies PerArmBandit`() {
        val r: PerArmBandit<RouletteWheelArmResult> = RouletteWheelBandit(nbrArms = 4)
        assertEquals(4, r.snapshot().size)
    }

    @Test
    fun `PerArmBandit snapshot length matches nbrArms across flavours`() {
        val bandits: List<PerArmBandit<*>> = listOf(
            MultiArmedBandit(nbrArms = 2, policy = NormalTS(), random = Random(1)),
            RouletteWheelBandit(nbrArms = 2),
            RegressionContextualBandit(
                nbrArms = 2,
                template = BayesianRegressionStat(featureSize = 1),
                posterior = MultivariateGaussian,
            ),
        )
        for (b in bandits) {
            val asBandit = b as Bandit
            assertEquals(asBandit.nbrArms, b.snapshot().size)
        }
    }

    @Test
    fun `Bandit reset returns to prior-seeded baseline`() {
        val mab = MultiArmedBandit(nbrArms = 2, policy = NormalTS(), random = Random(2))
        repeat(20) { mab.update(0, 5.0) }
        val before = mab.armResult(0).totalWeights
        (mab as Bandit).reset()
        val after = mab.armResult(0).totalWeights
        assertTrue(after < before)
    }

    @Test
    fun `PerArmBandit create returns a fresh independent replica`() {
        val original = MultiArmedBandit(nbrArms = 2, policy = NormalTS(), random = Random(3))
        repeat(20) { original.update(0, 5.0) }
        val copy = (original as PerArmBandit<*>).create(original.random)
        val populatedWeight = original.armResult(0).totalWeights

        @Suppress("UNCHECKED_CAST")
        val typedCopy = copy as MultiArmedBandit<*>
        val replicaWeight = (typedCopy.armResult(0) as
            com.eignex.kumulant.stat.summary.WeightedVarianceResult).totalWeights
        assertTrue(populatedWeight > replicaWeight)
    }

    @Test
    fun `Bandit random is exposed`() {
        val rng = Random(4)
        val b = MultiArmedBandit(nbrArms = 2, policy = NormalTS(), random = rng)
        val view: Bandit = b
        assertSame(rng, view.random)
    }

    @Test
    fun `PerArmBandit armResult and snapshot at the same index agree`() {
        val cb = RegressionContextualBandit(
            nbrArms = 3,
            template = BayesianRegressionStat(featureSize = 2),
            posterior = MultivariateGaussian,
            random = Random(5),
        )
        val x = DenseVector.of(doubleArrayOf(1.0, 0.0))
        cb.update(1, x, 2.0)
        val asBandit: PerArmBandit<*> = cb
        assertEquals(asBandit.snapshot()[1], asBandit.armResult(1))
    }

    // Suppresses unused warning on the BernoulliSumResult import (referenced by KDoc tests elsewhere).
    private val _bernoulliPlaceholder: BernoulliSumResult? = null
}
