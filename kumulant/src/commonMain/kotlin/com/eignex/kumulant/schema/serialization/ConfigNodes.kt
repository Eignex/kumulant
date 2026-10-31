package com.eignex.kumulant.schema.serialization

import com.eignex.kumulant.bandit.contextual.*
import com.eignex.kumulant.bandit.univariate.*
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.schema.optimizer.*
import com.eignex.kumulant.stat.regression.glm.*
import com.eignex.kumulant.stat.regression.tree.*
import com.eignex.skema.serialization.jsonNode
import kotlinx.serialization.PolymorphicSerializer

internal object OptimizerSpecSerializer : KumulantSerializer<OptimizerSpec>(OptimizerSpec::class, {
    listOf(
        jsonNode<Sgd>("sgd"),
        jsonNode<Adagrad>("adagrad"),
        jsonNode<Rmsprop>("rmsprop"),
        jsonNode<Adam>("adam"),
    )
})

internal object UnivariateBanditSpecSerializer : KumulantSerializer<UnivariateBanditSpec>(UnivariateBanditSpec::class, {
    listOf(
        jsonNode("multiArmed") { MultiArmedSpec.serializer(PolymorphicSerializer(Result::class)) },
        jsonNode<RouletteWheelSpec>("rouletteWheel"),
        jsonNode<BoltzmannSpec>("boltzmann"),
        jsonNode<Exp3Spec>("exp3"),
        jsonNode<TrackedUnivariateSpec>("tracked"),
        jsonNode("topTwoThompson") { TopTwoThompsonSpec.serializer(PolymorphicSerializer(Result::class)) },
    )
})

internal object ContextualBanditSpecSerializer : KumulantSerializer<ContextualBanditSpec>(ContextualBanditSpec::class, {
    listOf(
        jsonNode<TrackedContextualSpec>("tracked"),
        jsonNode("regressionContextual") { RegressionContextualSpec.serializer(PolymorphicSerializer(Result::class)) },
    )
})

internal object SerializableSplitSerializer : KumulantSerializer<SerializableSplit>(SerializableSplit::class, {
    listOf(
        jsonNode<ThresholdSplit>("thresholdSplit"),
        jsonNode<ExprSplit>("exprSplit"),
    )
})

internal object BanditPolicySpecSerializer : KumulantSerializer<BanditPolicySpec<*>>(BanditPolicySpec::class, {
    listOf(
        jsonNode("thompsonSampling") { ThompsonSamplingSpec.serializer(PolymorphicSerializer(Result::class)) },
        jsonNode<Ucb1Spec>("ucb1"),
        jsonNode<Ucb1NormalSpec>("ucb1Normal"),
        jsonNode<Ucb1TunedSpec>("ucb1Tuned"),
        jsonNode<GreedySpec>("greedy"),
        jsonNode<EpsilonGreedySpec>("epsilonGreedy"),
        jsonNode<EpsilonDecreasingSpec>("epsilonDecreasing"),
        jsonNode<UniformSelectionSpec>("uniformSelection"),
        jsonNode<KlUcbSpec>("klUcb"),
        jsonNode<MossSpec>("moss"),
        jsonNode<UcbVSpec>("ucbV"),
    )
})

internal object ArmSerializer : KumulantSerializer<Arm<*>>(Arm::class, {
    listOf(
        jsonNode<BernoulliArm>("bernoulliArm"),
        jsonNode<MeanArm>("meanArm"),
        jsonNode<NormalArm>("normalArm"),
        jsonNode<LogNormalArm>("logNormalArm"),
        jsonNode<MomentsArm>("momentsArm"),
        jsonNode<CompositeArm>("compositeArm"),
    )
})

internal object PosteriorSerializer : KumulantSerializer<Posterior<*>>(Posterior::class, {
    listOf(
        jsonNode<BetaPosterior>("betaPosterior"),
        jsonNode<PoissonGammaPosterior>("poissonGammaPosterior"),
        jsonNode<GeometricBetaPosterior>("geometricBetaPosterior"),
        jsonNode<ExponentialGammaPosterior>("exponentialGammaPosterior"),
        jsonNode<NormalGammaPosterior>("normalGammaPosterior"),
        jsonNode<LogNormalGammaPosterior>("logNormalGammaPosterior"),
        jsonNode<GammaScalePosterior>("gammaScalePosterior"),
        jsonNode<CompositePosterior>("compositePosterior"),
    )
})

internal object LinkSerializer : KumulantSerializer<Link>(Link::class, {
    listOf(
        jsonNode<Link.Identity>("identity"),
        jsonNode<Link.Logit>("logit"),
        jsonNode<Link.Log>("log"),
    )
})

internal object PenaltySerializer : KumulantSerializer<Penalty>(Penalty::class, {
    listOf(
        jsonNode<Penalty.None>("none"),
        jsonNode<Penalty.L1>("l1"),
        jsonNode<Penalty.L2>("l2"),
    )
})

internal object SplitMetricSerializer : KumulantSerializer<SplitMetric>(SplitMetric::class, {
    listOf(
        jsonNode<VarianceReduction>("varianceReduction"),
    )
})

internal object ClassificationSplitMetricSerializer : KumulantSerializer<ClassificationSplitMetric>(
    ClassificationSplitMetric::class,
    { listOf(jsonNode<GiniReduction>("giniReduction"), jsonNode<InformationGain>("informationGain")) },
)
