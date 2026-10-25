package com.eignex.kumulant.schema.serialization

import com.eignex.kumulant.bandit.contextual.*
import com.eignex.kumulant.bandit.univariate.*
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.schema.decay.*
import com.eignex.kumulant.schema.optimizer.*
import com.eignex.kumulant.stat.regression.glm.*
import com.eignex.kumulant.stat.regression.tree.*
import kotlinx.serialization.KSerializer
import kotlinx.serialization.PolymorphicSerializer

internal object DecayWeightingSpecSerializer : KSerializer<DecayWeightingSpec> by canonical(
    node<HalfLife>("halfLife"),
    node<Alpha>("alpha", WireShape.Operand, "alpha"),
)

internal object OptimizerSpecSerializer : KSerializer<OptimizerSpec> by canonical(
    node<Sgd>("sgd"),
    node<Adagrad>("adagrad"),
    node<Rmsprop>("rmsprop"),
    node<Adam>("adam"),
)

internal object UnivariateBanditSpecSerializer : KSerializer<UnivariateBanditSpec> by canonical(
    node("multiArmed") { MultiArmedSpec.serializer(PolymorphicSerializer(Result::class)) },
    node<RouletteWheelSpec>("rouletteWheel"),
    node<BoltzmannSpec>("boltzmann"),
    node<Exp3Spec>("exp3"),
    node<TrackedUnivariateSpec>("tracked"),
    node("topTwoThompson") { TopTwoThompsonSpec.serializer(PolymorphicSerializer(Result::class)) },
)

internal object ContextualBanditSpecSerializer : KSerializer<ContextualBanditSpec> by canonical(
    node<TrackedContextualSpec>("tracked"),
    node("regressionContextual") { RegressionContextualSpec.serializer(PolymorphicSerializer(Result::class)) },
)

internal object SerializableSplitSerializer : KSerializer<SerializableSplit> by canonical(
    node<ThresholdSplit>("thresholdSplit"),
    node<ExprSplit>("exprSplit"),
)

internal object BanditPolicySpecSerializer : KSerializer<BanditPolicySpec<*>> by canonical(
    node("thompsonSampling") { ThompsonSamplingSpec.serializer(PolymorphicSerializer(Result::class)) },
    node<Ucb1Spec>("ucb1"),
    node<Ucb1NormalSpec>("ucb1Normal"),
    node<Ucb1TunedSpec>("ucb1Tuned"),
    node<GreedySpec>("greedy"),
    node<EpsilonGreedySpec>("epsilonGreedy"),
    node<EpsilonDecreasingSpec>("epsilonDecreasing"),
    node<UniformSelectionSpec>("uniformSelection"),
    node<KlUcbSpec>("klUcb"),
    node<MossSpec>("moss"),
    node<UcbVSpec>("ucbV"),
)

internal object ArmSerializer : KSerializer<Arm<*>> by canonical(
    node<BernoulliArm>("bernoulliArm"),
    node<MeanArm>("meanArm"),
    node<NormalArm>("normalArm"),
    node<LogNormalArm>("logNormalArm"),
    node<MomentsArm>("momentsArm"),
    node<CompositeArm>("compositeArm"),
)

internal object PosteriorSerializer : KSerializer<Posterior<*>> by canonical(
    node<BetaPosterior>("betaPosterior"),
    node<PoissonGammaPosterior>("poissonGammaPosterior"),
    node<GeometricBetaPosterior>("geometricBetaPosterior"),
    node<ExponentialGammaPosterior>("exponentialGammaPosterior"),
    node<NormalGammaPosterior>("normalGammaPosterior"),
    node<LogNormalGammaPosterior>("logNormalGammaPosterior"),
    node<GammaScalePosterior>("gammaScalePosterior"),
    node<CompositePosterior>("compositePosterior"),
)

internal object LinkSerializer : KSerializer<Link> by canonical(
    node<Link.Identity>("identity"),
    node<Link.Logit>("logit"),
    node<Link.Log>("log"),
)

internal object PenaltySerializer : KSerializer<Penalty> by canonical(
    node<Penalty.None>("none"),
    node<Penalty.L1>("l1"),
    node<Penalty.L2>("l2"),
)

internal object SplitMetricSerializer : KSerializer<SplitMetric> by canonical(
    node<VarianceReduction>("varianceReduction"),
)

internal object ClassificationSplitMetricSerializer : KSerializer<ClassificationSplitMetric> by canonical(
    node<GiniReduction>("giniReduction"),
    node<InformationGain>("informationGain"),
)
