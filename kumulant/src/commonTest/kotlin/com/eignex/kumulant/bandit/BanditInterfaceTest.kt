package com.eignex.kumulant.bandit

import com.eignex.koblas.DenseVector
import com.eignex.kumulant.bandit.contextual.RegressionContextualBandit
import com.eignex.kumulant.bandit.univariate.MultiArmedBandit
import com.eignex.kumulant.bandit.univariate.NormalTS
import com.eignex.kumulant.bandit.univariate.RouletteWheelBandit
import com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat
import com.eignex.kumulant.stat.regression.glm.MultivariateGaussian
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class BanditInterfaceTest {

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
            val snapshot = b.snapshot()
            assertEquals(asBandit.nbrArms, snapshot.size)
            for (arm in snapshot.indices) assertEquals(snapshot[arm], b.armResult(arm))
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
        val replicaWeight = (
            typedCopy.armResult(0) as
                WeightedVarianceResult
            ).totalWeights
        assertTrue(populatedWeight > replicaWeight)
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
}
