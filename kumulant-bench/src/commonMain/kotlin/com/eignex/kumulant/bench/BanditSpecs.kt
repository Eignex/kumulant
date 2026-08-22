package com.eignex.kumulant.bench

import com.eignex.kumulant.bandit.Bandit
import com.eignex.kumulant.bandit.ContextualBandit
import com.eignex.kumulant.bandit.UnivariateBandit
import com.eignex.kumulant.bandit.contextual.RegressionContextualBandit
import com.eignex.kumulant.bandit.univariate.BetaBernoulliTS
import com.eignex.kumulant.bandit.univariate.MultiArmedBandit
import com.eignex.kumulant.bandit.univariate.UCB1
import com.eignex.koblas.core.F64DenseVector
import com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat
import com.eignex.kumulant.stat.regression.glm.MultivariateGaussian
import com.eignex.kumulant.stat.regression.glm.PointPosterior
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat
import com.eignex.kumulant.stat.regression.tree.DecisionTreeRegressionStat
import com.eignex.kumulant.stat.regression.tree.MeanTreePosterior
import com.eignex.kumulant.stat.regression.tree.ThresholdSplit
import com.eignex.kumulant.stat.regression.tree.RegressionTreeConfig
import kotlin.random.Random

/**
 * Bench-side description of a bandit configuration. Modality (univariate vs
 * contextual) is erased into the closures; there is one generic [BanditSpec]
 * class, and the factory functions [univariateBanditSpec] / [contextualBanditSpec]
 * pre-bake the choose/play/update logic. Mirrors the
 * [StatSpec][com.eignex.kumulant.bench.StatSpec] convention used for stat benches.
 */
class BanditSpec<B : Bandit>(
    /** Human-readable name printed in bench tables. */
    val name: String,
    /** Arm count fixed at spec construction time. */
    val nbrArms: Int,
    /** Context vector dimension; `0` for univariate. */
    val featureSize: Int,
    /** Build a fresh bandit with [random] as the PRNG. */
    val build: (Random) -> B,
    /** Run one (choose, play, update) round. Used by throughput / drift benches. */
    val cycle: (B, oracleRng: Random) -> Unit,
    /** Run one round and also compute the optimal-arm reward sample using [refRng].
     *  Used by the accuracy bench to compute per-round regret. */
    val regretCycle: (B, oracleRng: Random, refRng: Random) -> RegretSample,
)

/**
 * Per-round bookkeeping produced by [BanditSpec.regretCycle]: the chosen arm, the
 * oracle's optimal arm, the realized reward at the chosen arm, and the
 * counterfactual reward had the optimal arm been played.
 */
data class RegretSample(
    /** Arm the bandit chose this round. */
    val chosen: Int,
    /** Arm the oracle considers optimal for this round. */
    val optimal: Int,
    /** Realized reward at the chosen arm. */
    val reward: Double,
    /** Counterfactual reward at the optimal arm, sampled from the independent ref PRNG. */
    val optimalReward: Double,
)

/** Build a [BanditSpec] for a univariate bandit. The reward oracle is conditioned
 *  only on the arm index; [optimalArm] is the arm with the highest expected reward. */
fun univariateBanditSpec(
    name: String,
    nbrArms: Int,
    build: (Random) -> UnivariateBandit,
    sampleReward: (armIndex: Int, oracleRng: Random) -> Double,
    optimalArm: Int,
): BanditSpec<UnivariateBandit> = BanditSpec(
    name = name,
    nbrArms = nbrArms,
    featureSize = 0,
    build = build,
    cycle = { bandit, oracle ->
        val i = bandit.choose()
        val r = sampleReward(i, oracle)
        bandit.update(i, r)
    },
    regretCycle = { bandit, oracle, ref ->
        val i = bandit.choose()
        val r = sampleReward(i, oracle)
        bandit.update(i, r)
        RegretSample(i, optimalArm, r, sampleReward(optimalArm, ref))
    },
)

/** Build a [BanditSpec] for a contextual bandit. Each round samples a context
 *  via [sampleContext]; the reward oracle is conditioned on arm and context. */
fun contextualBanditSpec(
    name: String,
    nbrArms: Int,
    featureSize: Int,
    build: (Random) -> ContextualBandit,
    sampleContext: (oracleRng: Random) -> DoubleArray,
    sampleReward: (armIndex: Int, context: DoubleArray, oracleRng: Random) -> Double,
    optimalArm: (context: DoubleArray) -> Int,
): BanditSpec<ContextualBandit> = BanditSpec(
    name = name,
    nbrArms = nbrArms,
    featureSize = featureSize,
    build = build,
    cycle = { bandit, oracle ->
        val ctx = sampleContext(oracle)
        val x = F64DenseVector.of(ctx)
        val i = bandit.choose(x)
        val r = sampleReward(i, ctx, oracle)
        bandit.update(i, x, r)
    },
    regretCycle = { bandit, oracle, ref ->
        val ctx = sampleContext(oracle)
        val x = F64DenseVector.of(ctx)
        val i = bandit.choose(x)
        val r = sampleReward(i, ctx, oracle)
        bandit.update(i, x, r)
        val opt = optimalArm(ctx)
        RegretSample(i, opt, r, sampleReward(opt, ctx, ref))
    },
)

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

val multiArmedThompsonSpec = univariateBanditSpec(
    name = "MultiArmed/Thompson",
    nbrArms = 2,
    build = { r -> MultiArmedBandit(nbrArms = 2, policy = BetaBernoulliTS(), random = r) },
    sampleReward = ::sampleBernoulli,
    optimalArm = 1,
)

val multiArmedUcb1Spec = univariateBanditSpec(
    name = "MultiArmed/UCB1",
    nbrArms = 2,
    build = { r -> MultiArmedBandit(nbrArms = 2, policy = UCB1(), random = r) },
    sampleReward = ::sampleBernoulli,
    optimalArm = 1,
)

val bayesianContextualSpec = contextualBanditSpec(
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

val sgdContextualSpec = contextualBanditSpec(
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

val decisionTreeContextualSpec = contextualBanditSpec(
    name = "RegressionContextual/DT",
    nbrArms = 2,
    featureSize = 1,
    build = { r ->
        RegressionContextualBandit(
            nbrArms = 2,
            template = DecisionTreeRegressionStat(
                featureSize = 1,
                splitCandidates = treeBanditCandidates,
                config = RegressionTreeConfig(splitPeriod = 16, minSamplesSplit = 8.0, minSamplesLeaf = 4.0),
            ),
            posterior = MeanTreePosterior,
            random = r,
        )
    },
    sampleContext = ::contextualContext,
    sampleReward = ::contextualReward,
    optimalArm = ::contextualOptimalArm,
)

/** Every spec exposed by the bench module's bandit analyses. */
val allBanditSpecs: List<BanditSpec<*>> = listOf(
    multiArmedThompsonSpec,
    multiArmedUcb1Spec,
    bayesianContextualSpec,
    sgdContextualSpec,
    decisionTreeContextualSpec,
)
