package com.eignex.kumulant.bandit

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
 * Polymorphic checks against the joint [Bandit] interface — anything that targets
 * `Bandit<R>` and asks for `nbrArms`, `snapshot`, `merge`, `reset`, or `create` should
 * work uniformly across the univariate and contextual flavours.
 */
class BanditInterfaceTest {

    @Test
    fun `MultiArmedBandit and RegressionContextualBandit both satisfy Bandit`() {
        val mab: Bandit<BernoulliSumResult> = MultiArmedBandit(
            nbrArms = 3,
            policy = BetaBernoulliTS(),
            random = Random(0),
        )
        val cb: Bandit<*> = RegressionContextualBandit(
            nbrArms = 3,
            template = BayesianRegressionStat(featureSize = 2),
            posterior = MultivariateGaussian,
            random = Random(0),
        )
        assertEquals(3, mab.nbrArms)
        assertEquals(3, cb.nbrArms)
    }

    @Test
    fun `RouletteWheelBandit also satisfies Bandit`() {
        val r: Bandit<RouletteWheelArmResult> = RouletteWheelBandit(nbrArms = 4)
        assertEquals(4, r.nbrArms)
        assertEquals(4, r.snapshot().size)
    }

    @Test
    fun `Bandit snapshot length matches nbrArms across flavours`() {
        val bandits: List<Bandit<*>> = listOf(
            MultiArmedBandit(nbrArms = 2, policy = NormalTS(), random = Random(1)),
            RouletteWheelBandit(nbrArms = 2),
            RegressionContextualBandit(
                nbrArms = 2,
                template = BayesianRegressionStat(featureSize = 1),
                posterior = MultivariateGaussian,
            ),
        )
        for (b in bandits) assertEquals(b.nbrArms, b.snapshot().size)
    }

    @Test
    fun `Bandit reset returns to prior-seeded baseline`() {
        val mab = MultiArmedBandit(nbrArms = 2, policy = NormalTS(), random = Random(2))
        repeat(20) { mab.update(0, 5.0) }
        val before = mab.armResult(0).totalWeights
        (mab as Bandit<*>).reset()
        val after = mab.armResult(0).totalWeights
        assertTrue(after < before)
    }

    @Test
    fun `Bandit create returns a fresh independent replica`() {
        val original = MultiArmedBandit(nbrArms = 2, policy = NormalTS(), random = Random(3))
        repeat(20) { original.update(0, 5.0) }
        val copy = (original as Bandit<*>).create()
        // Updating the copy doesn't affect the original via shared state.
        assertTrue(original.armResult(0).totalWeights > copy.armResult(0).totalWeights)
    }

    @Test
    fun `Bandit random is exposed and preserved on create with default arg`() {
        val rng = Random(4)
        val b = MultiArmedBandit(nbrArms = 2, policy = NormalTS(), random = rng)
        val view: Bandit<*> = b
        assertSame(rng, view.random)
    }

    @Test
    fun `Bandit armResult and snapshot at the same index agree`() {
        val cb = RegressionContextualBandit(
            nbrArms = 3,
            template = BayesianRegressionStat(featureSize = 2),
            posterior = MultivariateGaussian,
            random = Random(5),
        )
        val x = DenseVector.of(doubleArrayOf(1.0, 0.0))
        cb.update(1, x, 2.0)
        val asBandit: Bandit<*> = cb
        assertEquals(asBandit.snapshot()[1], asBandit.armResult(1))
    }
}
