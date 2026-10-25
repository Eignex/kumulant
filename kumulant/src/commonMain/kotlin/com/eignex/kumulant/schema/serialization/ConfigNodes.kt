package com.eignex.kumulant.schema.serialization

import com.eignex.kumulant.bandit.contextual.*
import com.eignex.kumulant.bandit.univariate.*
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.schema.decay.*
import com.eignex.kumulant.schema.optimizer.*
import com.eignex.kumulant.stat.regression.glm.*
import com.eignex.kumulant.stat.regression.tree.*
import kotlinx.serialization.PolymorphicSerializer

internal object DecayWeightingSpecSerializer : CanonicalSerializer<DecayWeightingSpec>(
    DecayWeightingSpec::class,
    node<HalfLife>("halfLife"),
    node<Alpha>("alpha", WireShape.Operand, "alpha"),
)

internal object OptimizerSpecSerializer : CanonicalSerializer<OptimizerSpec>(
    OptimizerSpec::class,
    node<Sgd>("sgd"),
    node<Adagrad>("adagrad"),
    node<Rmsprop>("rmsprop"),
    node<Adam>("adam"),
)

internal object UnivariateBanditSpecSerializer : CanonicalSerializer<UnivariateBanditSpec>(
    UnivariateBanditSpec::class,
    node("multiArmed") { MultiArmedSpec.serializer(PolymorphicSerializer(Result::class)) },
    node<RouletteWheelSpec>("rouletteWheel"),
    node<BoltzmannSpec>("boltzmann"),
    node<Exp3Spec>("exp3"),
    node("topTwoThompson") { TopTwoThompsonSpec.serializer(PolymorphicSerializer(Result::class)) },
)

internal object ContextualBanditSpecSerializer : CanonicalSerializer<ContextualBanditSpec>(
    ContextualBanditSpec::class,
    node("regressionContextual") { RegressionContextualSpec.serializer(PolymorphicSerializer(Result::class)) },
)

internal object SerializableSplitSerializer : CanonicalSerializer<SerializableSplit>(
    SerializableSplit::class,
    node<ThresholdSplit>("thresholdSplit"),
    node<ExprSplit>("exprSplit"),
)

internal object BanditPolicySpecSerializer : CanonicalSerializer<BanditPolicySpec<*>>(
    BanditPolicySpec::class,
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

internal object ArmSerializer : CanonicalSerializer<Arm<*>>(
    Arm::class,
    node<BernoulliArm>("bernoulliArm"),
    node<MeanArm>("meanArm"),
    node<NormalArm>("normalArm"),
    node<LogNormalArm>("logNormalArm"),
    node<MomentsArm>("momentsArm"),
    node<CompositeArm>("compositeArm"),
)

internal object PosteriorSerializer : CanonicalSerializer<Posterior<*>>(
    Posterior::class,
    node<BetaPosterior>("betaPosterior"),
    node<PoissonGammaPosterior>("poissonGammaPosterior"),
    node<GeometricBetaPosterior>("geometricBetaPosterior"),
    node<ExponentialGammaPosterior>("exponentialGammaPosterior"),
    node<NormalGammaPosterior>("normalGammaPosterior"),
    node<LogNormalGammaPosterior>("logNormalGammaPosterior"),
    node<GammaScalePosterior>("gammaScalePosterior"),
    node<CompositePosterior>("compositePosterior"),
)

internal object LinkSerializer : CanonicalSerializer<Link>(
    Link::class,
    node<Link.Identity>("identity"),
    node<Link.Logit>("logit"),
    node<Link.Log>("log"),
)

internal object PenaltySerializer : CanonicalSerializer<Penalty>(
    Penalty::class,
    node<Penalty.None>("none"),
    node<Penalty.L1>("l1"),
    node<Penalty.L2>("l2"),
)

internal object SplitMetricSerializer : CanonicalSerializer<SplitMetric>(
    SplitMetric::class,
    node<VarianceReduction>("varianceReduction"),
)

internal object ClassificationSplitMetricSerializer : CanonicalSerializer<ClassificationSplitMetric>(
    ClassificationSplitMetric::class,
    node<GiniReduction>("giniReduction"),
    node<InformationGain>("informationGain"),
)
