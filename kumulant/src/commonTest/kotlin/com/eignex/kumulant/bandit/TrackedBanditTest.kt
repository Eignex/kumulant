package com.eignex.kumulant.bandit

import com.eignex.koblas.F64DenseVector
import com.eignex.kumulant.DELTA
import com.eignex.kumulant.bandit.contextual.RegressionContextualBandit
import com.eignex.kumulant.bandit.univariate.BetaBernoulliTS
import com.eignex.kumulant.bandit.univariate.MultiArmedBandit
import com.eignex.kumulant.stat.regression.CovarianceResult
import com.eignex.kumulant.stat.regression.CovarianceStat
import com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat
import com.eignex.kumulant.stat.regression.glm.MultivariateGaussian
import com.eignex.kumulant.stat.regression.glm.UnivariateRegressionResult
import com.eignex.kumulant.stat.regression.glm.UnivariateRegressionStat
import com.eignex.kumulant.stat.summary.CountStat
import com.eignex.kumulant.stat.summary.SumResult
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class TrackedBanditTest {

    @Test
    fun `contextual tracker drives all four slots`() {
        val inner = RegressionContextualBandit(
            nbrArms = 2,
            template = BayesianRegressionStat(featureSize = 1),
            posterior = MultivariateGaussian,
            random = Random(13),
        )
        val tracked = TrackedContextualBandit(
            inner = inner,
            contextFeatureSize = 1,
            chooseTemplate = BayesianRegressionStat(featureSize = 1), // policy: y = arm | x
            updateJointTemplate = BayesianRegressionStat(featureSize = 2), // 1 (arm) + 1 (context)
            updateMarginalTemplate = BayesianRegressionStat(featureSize = 1),
            updateArmRewardTemplate = CovarianceStat(),
        )
        val x = F64DenseVector.of(doubleArrayOf(0.3))
        tracked.update(0, x, reward = 1.0)
        tracked.update(0, x, reward = 0.5)
        tracked.update(1, x, reward = 0.0)
        repeat(4) { tracked.choose(x) }

        // All four slots produced a snapshot.
        assertNotNull(tracked.chooseResult())
        assertNotNull(tracked.updateJointResult())
        assertNotNull(tracked.updateMarginalResult())
        val armReward = tracked.updateArmRewardResult() as CovarianceResult
        assertEquals(3.0, armReward.totalWeights, DELTA)
        // Inner is still reachable for PerArmBandit / ContextualScorable.
        assertEquals(2, tracked.inner.nbrArms)
        assertNotNull(tracked.inner.armResult(0))
    }

    @Test
    fun `contextual tracker rejects joint template with wrong featureSize`() {
        val inner = RegressionContextualBandit(
            nbrArms = 2,
            template = BayesianRegressionStat(featureSize = 2),
            posterior = MultivariateGaussian,
            random = Random(0),
        )
        assertFailsWith<IllegalArgumentException> {
            TrackedContextualBandit(
                inner = inner,
                contextFeatureSize = 2,
                updateJointTemplate = BayesianRegressionStat(featureSize = 2), // should be 3
            )
        }
    }

    @Test
    fun `contextual tracker with all slots null is a pass-through`() {
        val inner = RegressionContextualBandit(
            nbrArms = 2,
            template = BayesianRegressionStat(featureSize = 1),
            posterior = MultivariateGaussian,
            random = Random(7),
        )
        val tracked = TrackedContextualBandit(inner = inner, contextFeatureSize = 1)
        val x = F64DenseVector.of(doubleArrayOf(0.1))
        tracked.update(0, x, reward = 1.0)
        tracked.choose(x)
        assertNull(tracked.chooseResult())
        assertNull(tracked.updateJointResult())
        assertNull(tracked.updateMarginalResult())
        assertNull(tracked.updateArmRewardResult())
    }

    @Test
    fun `univariate tracker records arm choice and arm-reward`() {
        val inner = MultiArmedBandit(nbrArms = 3, policy = BetaBernoulliTS(), random = Random(7))
        val tracked = TrackedUnivariateBandit(
            inner = inner,
            chooseTemplate = CountStat(),
            updateArmRewardTemplate = UnivariateRegressionStat(),
        )
        val plays = listOf(0 to 1.0, 0 to 1.0, 1 to 0.0, 2 to 1.0, 2 to 0.0, 2 to 1.0)
        for ((arm, r) in plays) tracked.update(arm, r)
        repeat(5) { tracked.choose() }

        val pickCount = (tracked.chooseResult() as SumResult).sum
        assertEquals(5.0, pickCount, DELTA)

        val armReward = tracked.updateArmRewardResult() as UnivariateRegressionResult
        assertEquals(6.0, armReward.totalWeights, DELTA)
    }

    @Test
    fun `univariate tracker with null templates disables tracking`() {
        val inner = MultiArmedBandit(nbrArms = 2, policy = BetaBernoulliTS(), random = Random(1))
        val tracked = TrackedUnivariateBandit(inner = inner)
        tracked.update(0, 1.0)
        tracked.choose()
        assertNull(tracked.chooseResult())
        assertNull(tracked.updateArmRewardResult())
    }

    @Test
    fun `reset clears all tracker slots`() {
        val inner = MultiArmedBandit(nbrArms = 2, policy = BetaBernoulliTS(), random = Random(1))
        val tracked = TrackedUnivariateBandit(
            inner = inner,
            chooseTemplate = CountStat(),
            updateArmRewardTemplate = UnivariateRegressionStat(),
        )
        tracked.update(0, 1.0)
        tracked.choose()
        assertTrue((tracked.chooseResult() as SumResult).sum > 0.0)
        assertTrue((tracked.updateArmRewardResult() as UnivariateRegressionResult).totalWeights > 0.0)
        tracked.reset()
        assertEquals(0.0, (tracked.chooseResult() as SumResult).sum, DELTA)
        assertEquals(0.0, (tracked.updateArmRewardResult() as UnivariateRegressionResult).totalWeights, DELTA)
    }
}
