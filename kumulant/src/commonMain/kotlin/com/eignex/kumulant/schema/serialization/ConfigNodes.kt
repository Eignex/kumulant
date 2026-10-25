@file:Suppress("UNCHECKED_CAST")

package com.eignex.kumulant.schema.serialization

import com.eignex.kumulant.bandit.contextual.*
import com.eignex.kumulant.bandit.univariate.*
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.schema.decay.*
import com.eignex.kumulant.schema.optimizer.*
import com.eignex.kumulant.stat.regression.glm.*
import com.eignex.kumulant.stat.regression.glm.LinearRegressionResult
import com.eignex.kumulant.stat.regression.glm.Link.*
import com.eignex.kumulant.stat.regression.glm.Penalty.*
import com.eignex.kumulant.stat.regression.tree.*
import kotlinx.serialization.KSerializer

internal val decayWeightingSpecNodes: List<WireNode> by lazy {
    listOf(
        WireNode(HalfLife::class, "halfLife", WireShape.Fields) { HalfLife.serializer() },
        WireNode(Alpha::class, "alpha", WireShape.Operand, listOf("alpha")) { Alpha.serializer() },
    )
}

internal object DecayWeightingSpecSerializer : CanonicalSerializer<DecayWeightingSpec>(
    DecayWeightingSpec::class,
    { decayWeightingSpecNodes },
)

internal val optimizerSpecNodes: List<WireNode> by lazy {
    listOf(
        WireNode(Sgd::class, "sgd", WireShape.Fields) { Sgd.serializer() },
        WireNode(Adagrad::class, "adagrad", WireShape.Fields) { Adagrad.serializer() },
        WireNode(Rmsprop::class, "rmsprop", WireShape.Fields) { Rmsprop.serializer() },
        WireNode(Adam::class, "adam", WireShape.Fields) { Adam.serializer() },
    )
}

internal object OptimizerSpecSerializer : CanonicalSerializer<OptimizerSpec>(
    OptimizerSpec::class,
    { optimizerSpecNodes },
)

internal val univariateBanditSpecNodes: List<WireNode> by lazy {
    listOf(
        WireNode(
            MultiArmedSpec::class,
            "multiArmed",
            WireShape.Fields,
        ) { MultiArmedSpec.serializer(kotlinx.serialization.PolymorphicSerializer(Result::class)) },
        WireNode(RouletteWheelSpec::class, "rouletteWheel", WireShape.Fields) { RouletteWheelSpec.serializer() },
        WireNode(BoltzmannSpec::class, "boltzmann", WireShape.Fields) { BoltzmannSpec.serializer() },
        WireNode(Exp3Spec::class, "exp3", WireShape.Fields) { Exp3Spec.serializer() },
        WireNode(
            TopTwoThompsonSpec::class,
            "topTwoThompson",
            WireShape.Fields,
        ) { TopTwoThompsonSpec.serializer(kotlinx.serialization.PolymorphicSerializer(Result::class)) },
    )
}

internal object UnivariateBanditSpecSerializer : CanonicalSerializer<UnivariateBanditSpec>(
    UnivariateBanditSpec::class,
    { univariateBanditSpecNodes },
)

internal val contextualBanditSpecNodes: List<WireNode> by lazy {
    listOf(
        WireNode(
            RegressionContextualSpec::class,
            "regressionContextual",
            WireShape.Fields,
        ) { RegressionContextualSpec.serializer(kotlinx.serialization.PolymorphicSerializer(Result::class)) },
    )
}

internal object ContextualBanditSpecSerializer : CanonicalSerializer<ContextualBanditSpec>(
    ContextualBanditSpec::class,
    { contextualBanditSpecNodes },
)

internal val serializableSplitNodes: List<WireNode> by lazy {
    listOf(
        WireNode(ThresholdSplit::class, "thresholdSplit", WireShape.Fields) { ThresholdSplit.serializer() },
        WireNode(ExprSplit::class, "exprSplit", WireShape.Fields) { ExprSplit.serializer() },
    )
}

internal object SerializableSplitSerializer : CanonicalSerializer<SerializableSplit>(
    SerializableSplit::class,
    { serializableSplitNodes },
)

internal val banditPolicySpecNodes: List<WireNode> by lazy {
    listOf(
        WireNode(
            ThompsonSamplingSpec::class,
            "thompsonSampling",
            WireShape.Fields,
        ) { ThompsonSamplingSpec.serializer(kotlinx.serialization.PolymorphicSerializer(Result::class)) },
        WireNode(Ucb1Spec::class, "ucb1", WireShape.Fields) { Ucb1Spec.serializer() },
        WireNode(Ucb1NormalSpec::class, "ucb1Normal", WireShape.Fields) { Ucb1NormalSpec.serializer() },
        WireNode(Ucb1TunedSpec::class, "ucb1Tuned", WireShape.Fields) { Ucb1TunedSpec.serializer() },
        WireNode(GreedySpec::class, "greedy", WireShape.Fields) { GreedySpec.serializer() },
        WireNode(EpsilonGreedySpec::class, "epsilonGreedy", WireShape.Fields) { EpsilonGreedySpec.serializer() },
        WireNode(
            EpsilonDecreasingSpec::class,
            "epsilonDecreasing",
            WireShape.Fields,
        ) { EpsilonDecreasingSpec.serializer() },
        WireNode(
            UniformSelectionSpec::class,
            "uniformSelection",
            WireShape.Fields,
        ) { UniformSelectionSpec.serializer() },
        WireNode(KlUcbSpec::class, "klUcb", WireShape.Fields) { KlUcbSpec.serializer() },
        WireNode(MossSpec::class, "moss", WireShape.Fields) { MossSpec.serializer() },
        WireNode(UcbVSpec::class, "ucbV", WireShape.Fields) { UcbVSpec.serializer() },
    )
}

@Suppress("UNUSED_PARAMETER")
internal class BanditPolicySpecSerializer<R : Result>(resultSerializer: KSerializer<R>) :
    CanonicalSerializer<BanditPolicySpec<*>>(
        BanditPolicySpec::class,
        { banditPolicySpecNodes },
    )

internal val armNodes: List<WireNode> by lazy {
    listOf(
        WireNode(BernoulliArm::class, "bernoulliArm", WireShape.Fields) { BernoulliArm.serializer() },
        WireNode(MeanArm::class, "meanArm", WireShape.Fields) { MeanArm.serializer() },
        WireNode(NormalArm::class, "normalArm", WireShape.Fields) { NormalArm.serializer() },
        WireNode(LogNormalArm::class, "logNormalArm", WireShape.Fields) { LogNormalArm.serializer() },
        WireNode(MomentsArm::class, "momentsArm", WireShape.Fields) { MomentsArm.serializer() },
        WireNode(CompositeArm::class, "compositeArm", WireShape.Fields) { CompositeArm.serializer() },
    )
}

@Suppress("UNUSED_PARAMETER")
internal class ArmSerializer<R : Result>(resultSerializer: KSerializer<R>) :
    CanonicalSerializer<Arm<*>>(
        Arm::class,
        { armNodes },
    )

internal val posteriorNodes: List<WireNode> by lazy {
    listOf(
        WireNode(BetaPosterior::class, "betaPosterior", WireShape.Fields) { BetaPosterior.serializer() },
        WireNode(
            PoissonGammaPosterior::class,
            "poissonGammaPosterior",
            WireShape.Fields,
        ) { PoissonGammaPosterior.serializer() },
        WireNode(
            GeometricBetaPosterior::class,
            "geometricBetaPosterior",
            WireShape.Fields,
        ) { GeometricBetaPosterior.serializer() },
        WireNode(
            ExponentialGammaPosterior::class,
            "exponentialGammaPosterior",
            WireShape.Fields,
        ) { ExponentialGammaPosterior.serializer() },
        WireNode(
            NormalGammaPosterior::class,
            "normalGammaPosterior",
            WireShape.Fields,
        ) { NormalGammaPosterior.serializer() },
        WireNode(
            LogNormalGammaPosterior::class,
            "logNormalGammaPosterior",
            WireShape.Fields,
        ) { LogNormalGammaPosterior.serializer() },
        WireNode(
            GammaScalePosterior::class,
            "gammaScalePosterior",
            WireShape.Fields,
        ) { GammaScalePosterior.serializer() },
        WireNode(CompositePosterior::class, "compositePosterior", WireShape.Fields) { CompositePosterior.serializer() },
    )
}

@Suppress("UNUSED_PARAMETER")
internal class PosteriorSerializer<R : Result>(resultSerializer: KSerializer<R>) :
    CanonicalSerializer<Posterior<*>>(
        Posterior::class,
        { posteriorNodes },
    )

internal val linearPosteriorNodes: List<WireNode> by lazy {
    listOf(
        WireNode(PointPosterior::class, "pointPosterior", WireShape.Fields) { PointPosterior.serializer() },
        WireNode(FactorisedGaussian::class, "factorisedGaussian", WireShape.Fields) { FactorisedGaussian.serializer() },
        WireNode(
            MultivariateGaussian::class,
            "multivariateGaussian",
            WireShape.Fields,
        ) { MultivariateGaussian.serializer() },
        WireNode(LinUcb::class, "linUcb", WireShape.Fields) { LinUcb.serializer() },
    )
}

@Suppress("UNUSED_PARAMETER")
internal class LinearPosteriorSerializer<R : LinearRegressionResult>(resultSerializer: KSerializer<R>) :
    CanonicalSerializer<LinearPosterior<*>>(
        LinearPosterior::class,
        { linearPosteriorNodes },
    )

internal val linkNodes: List<WireNode> by lazy {
    listOf(
        WireNode(Identity::class, "identity", WireShape.Fields) { Identity.serializer() },
        WireNode(Logit::class, "logit", WireShape.Fields) { Logit.serializer() },
        WireNode(Log::class, "log", WireShape.Fields) { Log.serializer() },
    )
}

internal object LinkSerializer : CanonicalSerializer<Link>(Link::class, { linkNodes })

internal val penaltyNodes: List<WireNode> by lazy {
    listOf(
        WireNode(None::class, "none", WireShape.Fields) { None.serializer() },
        WireNode(L1::class, "l1", WireShape.Fields) { L1.serializer() },
        WireNode(L2::class, "l2", WireShape.Fields) { L2.serializer() },
    )
}

internal object PenaltySerializer : CanonicalSerializer<Penalty>(Penalty::class, { penaltyNodes })

internal object SplitMetricSerializer : CanonicalSerializer<SplitMetric>(
    SplitMetric::class,
    {
        listOf(
            WireNode(VarianceReduction::class, "varianceReduction") { VarianceReduction.serializer() },
        )
    },
)

internal object ClassificationSplitMetricSerializer : CanonicalSerializer<ClassificationSplitMetric>(
    ClassificationSplitMetric::class,
    {
        listOf(
            WireNode(GiniReduction::class, "giniReduction") { GiniReduction.serializer() },
            WireNode(InformationGain::class, "informationGain") { InformationGain.serializer() },
        )
    },
)
