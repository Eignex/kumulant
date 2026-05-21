package com.eignex.kumulant.bench

import com.eignex.kumulant.bandit.Bandit
import com.eignex.kumulant.bandit.ContextualBandit
import com.eignex.kumulant.bandit.UnivariateBandit
import com.eignex.kumulant.bandit.contextual.RegressionContextualBandit
import com.eignex.kumulant.bandit.univariate.BetaBernoulliTS
import com.eignex.kumulant.bandit.univariate.MultiArmedBandit
import com.eignex.kumulant.bandit.univariate.UCB1
import com.eignex.kumulant.math.DenseVector
import com.eignex.kumulant.stat.regression.BayesianRegressionStat
import com.eignex.kumulant.stat.regression.MultivariateGaussian
import com.eignex.kumulant.stat.regression.PointPosterior
import com.eignex.kumulant.stat.regression.StochasticRegressionStat
import com.eignex.kumulant.stat.tree.DecisionTreeRegressionStat
import com.eignex.kumulant.stat.tree.ThresholdSplit
import com.eignex.kumulant.stat.tree.TreeConfig
import kotlin.random.Random

/**
 * Bench-side description of a bandit configuration. Carries the constructors plus
 * a synthetic reward oracle so the bench can drive (choose, play, update) cycles
 * end-to-end without depending on a live environment.
 *
 * Two variants — univariate and contextual — because the action/feedback shape
 * differs at the bandit interface. Both share name and arm count.
 */
sealed interface BanditSpec {
    /** Human-readable spec name; printed in bench tables. */
    val name: String

    /** Arms in the population; fixed at spec construction time. */
    val nbrArms: Int

    /** Build a fresh bandit + per-cycle driver. The driver closes over [random]
     *  (bandit PRNG) and [oracleRng] (reward sampling), so multiple drivers can
     *  share a spec safely. */
    fun newDriver(random: Random, oracleRng: Random): BanditDriver
}

/**
 * A bandit instance together with a `cycle` closure that performs one
 * (choose, play, update) round. Bench analyses call [cycle] in tight loops.
 */
class BanditDriver(
    /** Live bandit; expose so analyses can snapshot when needed. */
    val bandit: Bandit,
    /** Run one (choose, play, update) round. */
    val cycle: () -> Unit,
)

/**
 * Univariate bandit spec: no context, arm chosen by [Bandit.choose] alone. The
 * oracle yields a scalar reward per arm via [sampleReward]. [optimalArm] is the
 * arm with the highest expected reward under the oracle and is used by the
 * accuracy analysis as the regret baseline.
 */
class UnivariateBanditSpec(
    override val name: String,
    override val nbrArms: Int,
    /** Factory taking a PRNG; one fresh bandit per driver. */
    val build: (Random) -> UnivariateBandit,
    /** Reward sampler conditioned on arm index. */
    val sampleReward: (armIndex: Int, oracleRng: Random) -> Double,
    /** Best arm under the oracle's expected reward; used by the accuracy bench. */
    val optimalArm: Int,
) : BanditSpec {
    override fun newDriver(random: Random, oracleRng: Random): BanditDriver {
        val bandit = build(random)
        return BanditDriver(bandit) {
            val i = bandit.choose()
            val r = sampleReward(i, oracleRng)
            bandit.update(i, r)
        }
    }
}

/**
 * Contextual bandit spec: each round samples a context vector and the chosen
 * arm's reward conditional on context. [optimalArm] for accuracy lookups is a
 * function of the context vector.
 */
class ContextualBanditSpec(
    override val name: String,
    override val nbrArms: Int,
    /** Context dimension. */
    val featureSize: Int,
    /** Factory taking a PRNG; one fresh bandit per driver. */
    val build: (Random) -> ContextualBandit,
    /** Per-round context sampler. */
    val sampleContext: (oracleRng: Random) -> DoubleArray,
    /** Reward sampler conditioned on arm and context. */
    val sampleReward: (armIndex: Int, context: DoubleArray, oracleRng: Random) -> Double,
    /** Best arm given context under the oracle's expected reward. */
    val optimalArm: (context: DoubleArray) -> Int,
) : BanditSpec {
    override fun newDriver(random: Random, oracleRng: Random): BanditDriver {
        val bandit = build(random)
        return BanditDriver(bandit) {
            val ctx = sampleContext(oracleRng)
            val x = DenseVector.of(ctx)
            val i = bandit.choose(x)
            val r = sampleReward(i, ctx, oracleRng)
            bandit.update(i, x, r)
        }
    }
}

// Catalog ----------------------------------------------------------------------

/** Reward model for the univariate specs: two-armed Bernoulli with optimal arm 1. */
private val bernoulliRewards = doubleArrayOf(0.3, 0.7)
private fun sampleBernoulli(armIndex: Int, oracleRng: Random): Double =
    if (oracleRng.nextDouble() < bernoulliRewards[armIndex]) 1.0 else 0.0

/** Reward model for contextual specs: arm 0 wins when x[0] >= 0, arm 1 otherwise.
 *  Reward is `1.0` on the correct arm plus Gaussian noise; `0.0 + noise` on the wrong. */
private fun contextualReward(armIndex: Int, x: DoubleArray, oracleRng: Random): Double {
    val correct = if (x[0] >= 0.0) 0 else 1
    val signal = if (armIndex == correct) 1.0 else 0.0
    return signal + oracleRng.nextDouble() * 0.1
}
private fun contextualOptimalArm(x: DoubleArray): Int = if (x[0] >= 0.0) 0 else 1
private fun contextualContext(oracleRng: Random): DoubleArray =
    doubleArrayOf(oracleRng.nextDouble() * 2.0 - 1.0)

val multiArmedThompsonSpec = UnivariateBanditSpec(
    name = "MultiArmed/Thompson",
    nbrArms = 2,
    build = { r -> MultiArmedBandit(nbrArms = 2, policy = BetaBernoulliTS(), random = r) },
    sampleReward = ::sampleBernoulli,
    optimalArm = 1,
)

val multiArmedUcb1Spec = UnivariateBanditSpec(
    name = "MultiArmed/UCB1",
    nbrArms = 2,
    build = { r -> MultiArmedBandit(nbrArms = 2, policy = UCB1(), random = r) },
    sampleReward = ::sampleBernoulli,
    optimalArm = 1,
)

val bayesianContextualSpec = ContextualBanditSpec(
    name = "RegressionContextual/Bayesian",
    nbrArms = 2,
    featureSize = 1,
    build = { r ->
        RegressionContextualBandit(
            nbrArms = 2,
            template = BayesianRegressionStat(featureSize = 1),
            posterior = MultivariateGaussian,
            random = r,
        )
    },
    sampleContext = ::contextualContext,
    sampleReward = ::contextualReward,
    optimalArm = ::contextualOptimalArm,
)

val sgdContextualSpec = ContextualBanditSpec(
    name = "RegressionContextual/SGD",
    nbrArms = 2,
    featureSize = 1,
    build = { r ->
        RegressionContextualBandit(
            nbrArms = 2,
            template = StochasticRegressionStat(featureSize = 1),
            posterior = PointPosterior,
            exploration = 0.0,
            random = r,
        )
    },
    sampleContext = ::contextualContext,
    sampleReward = ::contextualReward,
    optimalArm = ::contextualOptimalArm,
)

private val treeBanditCandidates = listOf(
    ThresholdSplit(0, -0.5),
    ThresholdSplit(0, 0.0),
    ThresholdSplit(0, 0.5),
)

val decisionTreeContextualSpec = ContextualBanditSpec(
    name = "RegressionContextual/DT",
    nbrArms = 2,
    featureSize = 1,
    build = { r ->
        RegressionContextualBandit(
            nbrArms = 2,
            template = DecisionTreeRegressionStat(
                featureSize = 1,
                splitCandidates = treeBanditCandidates,
                config = TreeConfig(splitPeriod = 16, minSamplesSplit = 8.0, minSamplesLeaf = 4.0),
            ),
            posterior = com.eignex.kumulant.stat.tree.MeanTreePosterior,
            random = r,
        )
    },
    sampleContext = ::contextualContext,
    sampleReward = ::contextualReward,
    optimalArm = ::contextualOptimalArm,
)

/** Every spec exposed by the bench module's bandit analyses. */
val allBanditSpecs: List<BanditSpec> = listOf(
    multiArmedThompsonSpec,
    multiArmedUcb1Spec,
    bayesianContextualSpec,
    sgdContextualSpec,
    decisionTreeContextualSpec,
)
