package com.eignex.kumulant.schema.serialization

private val serializersByName by lazy {
    val roots = listOf(
        StatSpecSerializer,
        ScalarExprSerializer,
        BoolExprSerializer,
        VectorExprSerializer,
        DecayWeightingSpecSerializer,
        OptimizerSpecSerializer,
        UnivariateBanditSpecSerializer,
        ContextualBanditSpecSerializer,
        SerializableSplitSerializer,
        BanditPolicySpecSerializer,
        ArmSerializer,
        PosteriorSerializer,
        LinearPosteriorSerializer,
        LinkSerializer,
        PenaltySerializer,
        SplitMetricSerializer,
        ClassificationSplitMetricSerializer,
        TreePosteriorSerializer,
        ForestPosteriorSerializer,
        CanonicalRegressionPosteriorSerializer,
    )
    (statSerializers + roots).associateBy { it.descriptor.serialName }
}

internal fun wireNodes(serialName: String): List<WireNode>? = serializersByName[serialName]?.nodes
