package com.eignex.kumulant.bandit

import com.eignex.koblas.F64VectorView
import com.eignex.kumulant.bandit.contextual.KnnContextualBandit
import com.eignex.kumulant.bandit.contextual.RegressionContextualBandit
import com.eignex.kumulant.bandit.univariate.BernoulliArm
import com.eignex.kumulant.bandit.univariate.BetaPosterior
import com.eignex.kumulant.bandit.univariate.BoltzmannBandit
import com.eignex.kumulant.bandit.univariate.Exp3Bandit
import com.eignex.kumulant.bandit.univariate.MultiArmedBandit
import com.eignex.kumulant.bandit.univariate.RouletteWheelBandit
import com.eignex.kumulant.bandit.univariate.ThompsonSampling
import com.eignex.kumulant.bandit.univariate.TopTwoThompsonBandit
import com.eignex.kumulant.feat
import com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat
import com.eignex.kumulant.stat.regression.glm.MultivariateGaussian
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals

private const val ARMS = 3

// Swept across the whole family rather than restated per bandit, because the failure mode is one
// member being left out - which is exactly what a per-bandit test misses.
class ArmIndexContractSweepTest {

    private class Probe(val name: String, val calls: List<Pair<String, (Int) -> Any?>>)

    private val x: F64VectorView = feat(1.0, 1.0)

    private fun probes(): List<Probe> {
        val multiArmed = MultiArmedBandit(ARMS, ThompsonSampling(BernoulliArm(), BetaPosterior), Random(0))
        val roulette = RouletteWheelBandit(ARMS, random = Random(0))
        val boltzmann = BoltzmannBandit(ARMS, random = Random(0))
        val exp3 = Exp3Bandit(ARMS, random = Random(0))
        val topTwo = TopTwoThompsonBandit(ARMS, ThompsonSampling(BernoulliArm(), BetaPosterior), random = Random(0))
        val knn = KnnContextualBandit(ARMS, random = Random(0))
        val regression = RegressionContextualBandit(
            nbrArms = ARMS,
            template = BayesianRegressionStat(featureSize = 2),
            posterior = MultivariateGaussian,
            random = Random(0),
        )
        return listOf(
            Probe(
                "MultiArmedBandit",
                listOf(
                    "update" to { i -> multiArmed.update(i, 1.0) },
                    "evaluate" to { i -> multiArmed.evaluate(i) },
                    "armResult" to { i -> multiArmed.armResult(i) },
                    "armStat" to { i -> multiArmed.armStat(i) },
                ),
            ),
            Probe(
                "RouletteWheelBandit",
                listOf(
                    "update" to { i -> roulette.update(i, 1.0) },
                    "evaluate" to { i -> roulette.evaluate(i) },
                ),
            ),
            Probe(
                "BoltzmannBandit",
                listOf(
                    "update" to { i -> boltzmann.update(i, 1.0) },
                ),
            ),
            Probe(
                "Exp3Bandit",
                listOf(
                    "update" to { i -> exp3.update(i, 1.0) },
                ),
            ),
            Probe(
                "TopTwoThompsonBandit",
                listOf(
                    "update" to { i -> topTwo.update(i, 1.0) },
                ),
            ),
            Probe(
                "KnnContextualBandit",
                listOf(
                    "update" to { i -> knn.update(i, x, 1.0) },
                    "evaluate" to { i -> knn.evaluate(i, x) },
                    "historySize" to { i -> knn.historySize(i) },
                    "armWeight" to { i -> knn.armWeight(i) },
                ),
            ),
            Probe(
                "RegressionContextualBandit",
                listOf(
                    "update" to { i -> regression.update(i, x, 1.0) },
                    "evaluate" to { i -> regression.evaluate(i, x) },
                    "armResult" to { i -> regression.armResult(i) },
                    "armStat" to { i -> regression.armStat(i) },
                ),
            ),
        )
    }

    @Test
    fun `every arm-indexed call rejects an out-of-range index`() {
        // Negative and one-past-the-end. The negative case is the one a raw array access would also
        // catch; `nbrArms` exactly is the one that reads as plausible and must still be refused.
        val violations = mutableListOf<String>()
        for (bad in listOf(-1, ARMS)) {
            for (probe in probes()) {
                for ((call, invoke) in probe.calls) {
                    val thrown = runCatching { invoke(bad) }.exceptionOrNull()
                    when {
                        thrown == null -> violations += "${probe.name}.$call accepted index $bad"

                        thrown !is IllegalArgumentException ->
                            violations += "${probe.name}.$call threw ${thrown::class.simpleName} for $bad"
                    }
                }
            }
        }
        assertEquals(emptyList(), violations.toList(), "the documented IllegalArgumentException was not raised")
    }

    @Test
    fun `every arm-indexed call accepts every in-range index`() {
        // Guards the rule above: a bandit that rejected every index would satisfy it.
        val violations = mutableListOf<String>()
        for (good in 0 until ARMS) {
            for (probe in probes()) {
                for ((call, invoke) in probe.calls) {
                    runCatching { invoke(good) }.exceptionOrNull()?.let {
                        violations += "${probe.name}.$call rejected valid index $good: ${it.message}"
                    }
                }
            }
        }
        assertEquals(emptyList(), violations.toList(), "a valid arm index was refused")
    }
}
