package com.eignex.kumulant.bench

import com.eignex.kumulant.bandit.Bandit
import com.eignex.kumulant.bandit.contextual.RegressionContextualBandit
import com.eignex.kumulant.bandit.univariate.BetaBernoulliTS
import com.eignex.kumulant.bandit.univariate.MultiArmedBandit
import com.eignex.kumulant.bandit.univariate.UCB1
import com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat
import com.eignex.kumulant.stat.regression.glm.MultivariateGaussian
import com.eignex.kumulant.stat.regression.glm.PointPosterior
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat
import com.eignex.kumulant.stat.regression.tree.DecisionTreeRegressionStat
import com.eignex.kumulant.stat.regression.tree.MeanTreePosterior
import com.eignex.kumulant.stat.regression.tree.ThresholdSplit
import com.eignex.kumulant.stat.regression.tree.RegressionTreeConfig
import kotlin.random.Random

internal class BanditSpec<B : Bandit>(
    val name: String,
    val featureSize: Int = 0,
    val build: (Random) -> B,
) {
    val nbrArms: Int = 2
}

internal val multiArmedThompsonSpec = BanditSpec(
    name = "MultiArmed/Thompson",
    build = { r -> MultiArmedBandit(nbrArms = 2, policy = BetaBernoulliTS(), random = r) },
)

internal val multiArmedUcb1Spec = BanditSpec(
    name = "MultiArmed/UCB1",
    build = { r -> MultiArmedBandit(nbrArms = 2, policy = UCB1(), random = r) },
)

internal val bayesianContextualSpec = BanditSpec(
    name = "RegressionContextual/Bayesian",
    featureSize = 1,
    build = { r ->
        RegressionContextualBandit(
            nbrArms = 2,
            template = BayesianRegressionStat(featureSize = 1),
            posterior = MultivariateGaussian,
            random = r,
        )
    },
)

internal val sgdContextualSpec = BanditSpec(
    name = "RegressionContextual/SGD",
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
)

private val treeBanditCandidates = listOf(
    ThresholdSplit(0, -0.5),
    ThresholdSplit(0, 0.0),
    ThresholdSplit(0, 0.5),
)

internal val decisionTreeContextualSpec = BanditSpec(
    name = "RegressionContextual/DT",
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
)

internal val allBanditSpecs: List<BanditSpec<*>> = listOf(
    multiArmedThompsonSpec,
    multiArmedUcb1Spec,
    bayesianContextualSpec,
    sgdContextualSpec,
    decisionTreeContextualSpec,
)
