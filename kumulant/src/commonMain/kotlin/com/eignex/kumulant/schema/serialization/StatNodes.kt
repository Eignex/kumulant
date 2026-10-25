package com.eignex.kumulant.schema.serialization

import com.eignex.kumulant.schema.spec.*

internal val seriesStatSpecNodes: List<WireNode> by lazy {
    listOf(
        WireNode(Mean::class, "mean") { Mean.serializer() },
        WireNode(Sum::class, "sum") { Sum.serializer() },
        WireNode(Min::class, "min") { Min.serializer() },
        WireNode(Max::class, "max") { Max.serializer() },
        WireNode(ArgMin::class, "argMin") { ArgMin.serializer() },
        WireNode(ArgMax::class, "argMax") { ArgMax.serializer() },
        WireNode(Range::class, "range") { Range.serializer() },
        WireNode(Excursion::class, "excursion") { Excursion.serializer() },
        WireNode(RunLength::class, "runLength") { RunLength.serializer() },
        WireNode(Recency::class, "recency") { Recency.serializer() },
        WireNode(Crossing::class, "crossing") { Crossing.serializer() },
        WireNode(Cusum::class, "cusum") { Cusum.serializer() },
        WireNode(PageHinkley::class, "pageHinkley") { PageHinkley.serializer() },
        WireNode(Adwin::class, "adwin") { Adwin.serializer() },
        WireNode(Mad::class, "mad") { Mad.serializer() },
        WireNode(FrugalMad::class, "frugalMad") { FrugalMad.serializer() },
        WireNode(ThresholdBucket::class, "thresholdBucket") { ThresholdBucket.serializer() },
        WireNode(Variance::class, "variance") { Variance.serializer() },
        WireNode(Moments::class, "moments") { Moments.serializer() },
        WireNode(ClassCounts::class, "classCounts") { ClassCounts.serializer() },
        WireNode(Summary::class, "summary") { Summary.serializer() },
        WireNode(BernoulliSum::class, "bernoulliSum") { BernoulliSum.serializer() },
        WireNode(TotalWeights::class, "totalWeights") { TotalWeights.serializer() },
        WireNode(Count::class, "count") { Count.serializer() },
        WireNode(Rate::class, "rate") { Rate.serializer() },
        WireNode(CounterRate::class, "counterRate") { CounterRate.serializer() },
        WireNode(DDSketch::class, "ddSketch") { DDSketch.serializer() },
        WireNode(FrugalQuantile::class, "frugalQuantile") { FrugalQuantile.serializer() },
        WireNode(HdrHistogram::class, "hdrHistogram") { HdrHistogram.serializer() },
        WireNode(LinearHistogram::class, "linearHistogram") { LinearHistogram.serializer() },
        WireNode(ReservoirHistogram::class, "reservoirHistogram") { ReservoirHistogram.serializer() },
        WireNode(TDigest::class, "tDigest") { TDigest.serializer() },
        WireNode(PitHistogram::class, "pitHistogram") { PitHistogram.serializer() },
        WireNode(WithWeightSeries::class, "withWeight") { WithWeightSeries.serializer() },
        WireNode(WithValueSeries::class, "withValue") { WithValueSeries.serializer() },
        WireNode(AsSeries::class, "asSeries") { AsSeries.serializer() },
        WireNode(WithFixedX::class, "withFixedX") { WithFixedX.serializer() },
        WireNode(WithFixedY::class, "withFixedY") { WithFixedY.serializer() },
        WireNode(WithTimeAsX::class, "withTimeAsX") { WithTimeAsX.serializer() },
        WireNode(WithTimeAsY::class, "withTimeAsY") { WithTimeAsY.serializer() },
        WireNode(WindowedSeries::class, "window") { WindowedSeries.serializer() },
        WireNode(TransformValueSeries::class, "transform") { TransformValueSeries.serializer() },
        WireNode(FilterValueSeries::class, "filter", WireShape.Filter) { FilterValueSeries.serializer() },
        WireNode(WeightByValueSeries::class, "weightBy") { WeightByValueSeries.serializer() },
        WireNode(ThrottleSeries::class, "throttle") { ThrottleSeries.serializer() },
        WireNode(LagSeries::class, "lag") { LagSeries.serializer() },
        WireNode(DiffSeries::class, "diff") { DiffSeries.serializer() },
        WireNode(DerivativeSeries::class, "derivative") { DerivativeSeries.serializer() },
        WireNode(HysteresisSeries::class, "hysteresis") { HysteresisSeries.serializer() },
        WireNode(ResampleByTimeSeries::class, "resampleByTime") { ResampleByTimeSeries.serializer() },
        WireNode(BandSeries::class, "band") { BandSeries.serializer() },
        WireNode(WithSelfLagSeries::class, "selfLag") { WithSelfLagSeries.serializer() },
        WireNode(WithFeedbackSeries::class, "feedback") { WithFeedbackSeries.serializer() },
        WireNode(StandardScalerSeries::class, "standardScaler") { StandardScalerSeries.serializer() },
        WireNode(MinMaxScalerSeries::class, "minMaxScaler") { MinMaxScalerSeries.serializer() },
        WireNode(SampleSeries::class, "sample") { SampleSeries.serializer() },
        WireNode(GroupStatSpec::class, "group") { GroupStatSpec.serializer() },
        WireNode(
            DecayingSum::class,
            "decayingSum",
            WireShape.Decay,
            listOf("halfLifeMillis"),
        ) { DecayingSum.serializer() },
        WireNode(
            DecayingMean::class,
            "decayingMean",
            WireShape.Decay,
            listOf("halfLifeMillis"),
        ) { DecayingMean.serializer() },
        WireNode(
            DecayingVariance::class,
            "decayingVariance",
            WireShape.Decay,
            listOf("halfLifeMillis"),
        ) { DecayingVariance.serializer() },
        WireNode(EwmaMean::class, "ewma", WireShape.Decay, listOf("alpha")) { EwmaMean.serializer() },
        WireNode(EwmaVariance::class, "ewmaVariance", WireShape.Decay, listOf("alpha")) { EwmaVariance.serializer() },
        WireNode(Holt::class, "holt", WireShape.Smoothing) { Holt.serializer() },
        WireNode(SeasonalSmoothing::class, "seasonalSmoothing", WireShape.Smoothing) { SeasonalSmoothing.serializer() },
        WireNode(RecursiveVariance::class, "recursiveVariance") { RecursiveVariance.serializer() },
        WireNode(DecayingRate::class, "decayingRate") { DecayingRate.serializer() },
    )
}

internal val pairedStatSpecNodes: List<WireNode> by lazy {
    listOf(
        WireNode(PairedSum::class, "pairedSum") { PairedSum.serializer() },
        WireNode(UnivariateRegression::class, "univariateRegression") { UnivariateRegression.serializer() },
        WireNode(Covariance::class, "covariance") { Covariance.serializer() },
        WireNode(MseLoss::class, "mseLoss") { MseLoss.serializer() },
        WireNode(MaeLoss::class, "maeLoss") { MaeLoss.serializer() },
        WireNode(LogLoss::class, "logLoss") { LogLoss.serializer() },
        WireNode(PinballLoss::class, "pinballLoss") { PinballLoss.serializer() },
        WireNode(Auc::class, "auc") { Auc.serializer() },
        WireNode(Reliability::class, "reliability") { Reliability.serializer() },
        WireNode(PlattCalibrator::class, "plattCalibrator") { PlattCalibrator.serializer() },
        WireNode(ConfusionMatrix::class, "confusionMatrix") { ConfusionMatrix.serializer() },
        WireNode(Accuracy::class, "accuracy") { Accuracy.serializer() },
        WireNode(WithWeightPaired::class, "withWeight") { WithWeightPaired.serializer() },
        WireNode(AtX::class, "atX") { AtX.serializer() },
        WireNode(AtY::class, "atY") { AtY.serializer() },
        WireNode(WindowedPaired::class, "window") { WindowedPaired.serializer() },
        WireNode(TransformPair::class, "transformPair") { TransformPair.serializer() },
        WireNode(FilterPaired::class, "filter", WireShape.Filter) { FilterPaired.serializer() },
        WireNode(FoldPaired::class, "foldPaired") { FoldPaired.serializer() },
        WireNode(WeightByValuePaired::class, "weightBy") { WeightByValuePaired.serializer() },
        WireNode(ThrottlePaired::class, "throttle") { ThrottlePaired.serializer() },
        WireNode(StandardScalerPaired::class, "standardScaler") { StandardScalerPaired.serializer() },
        WireNode(MinMaxScalerPaired::class, "minMaxScaler") { MinMaxScalerPaired.serializer() },
        WireNode(SamplePaired::class, "sample") { SamplePaired.serializer() },
    )
}

internal val vectorStatSpecNodes: List<WireNode> by lazy {
    listOf(
        WireNode(WithWeightVector::class, "withWeight") { WithWeightVector.serializer() },
        WireNode(AtIndex::class, "atIndex") { AtIndex.serializer() },
        WireNode(AtIndices::class, "atIndices") { AtIndices.serializer() },
        WireNode(WindowedVector::class, "window") { WindowedVector.serializer() },
        WireNode(Vectorized::class, "vectorize") { Vectorized.serializer() },
        WireNode(TransformVectorElement::class, "transformElements") { TransformVectorElement.serializer() },
        WireNode(FilterVector::class, "filter", WireShape.Filter) { FilterVector.serializer() },
        WireNode(FoldVector::class, "foldVector") { FoldVector.serializer() },
        WireNode(FoldVectorPaired::class, "foldVectorPaired") { FoldVectorPaired.serializer() },
        WireNode(TransformVector::class, "transformVector") { TransformVector.serializer() },
        WireNode(WeightByValueVector::class, "weightBy") { WeightByValueVector.serializer() },
        WireNode(ThrottleVector::class, "throttle") { ThrottleVector.serializer() },
        WireNode(StandardScalerVector::class, "standardScaler") { StandardScalerVector.serializer() },
        WireNode(MinMaxScalerVector::class, "minMaxScaler") { MinMaxScalerVector.serializer() },
        WireNode(SampleVector::class, "sample") { SampleVector.serializer() },
    )
}

internal val discreteStatSpecNodes: List<WireNode> by lazy {
    listOf(
        WireNode(HyperLogLog::class, "hyperLogLog") { HyperLogLog.serializer() },
        WireNode(LinearCounting::class, "linearCounting") { LinearCounting.serializer() },
        WireNode(BloomFilter::class, "bloomFilter") { BloomFilter.serializer() },
        WireNode(CountMinSketch::class, "countMinSketch") { CountMinSketch.serializer() },
        WireNode(MinHash::class, "minHash") { MinHash.serializer() },
        WireNode(Sojourn::class, "sojourn") { Sojourn.serializer() },
        WireNode(SpaceSaving::class, "spaceSaving") { SpaceSaving.serializer() },
        WireNode(WithWeightDiscrete::class, "withWeight") { WithWeightDiscrete.serializer() },
        WireNode(WithValueDiscrete::class, "withValue") { WithValueDiscrete.serializer() },
        WireNode(AsDiscrete::class, "asDiscrete") { AsDiscrete.serializer() },
        WireNode(WindowedDiscrete::class, "window") { WindowedDiscrete.serializer() },
        WireNode(TransformValueDiscrete::class, "transform") { TransformValueDiscrete.serializer() },
        WireNode(FilterValueDiscrete::class, "filter", WireShape.Filter) { FilterValueDiscrete.serializer() },
        WireNode(WeightByValueDiscrete::class, "weightBy") { WeightByValueDiscrete.serializer() },
        WireNode(ThrottleDiscrete::class, "throttle") { ThrottleDiscrete.serializer() },
        WireNode(SampleDiscrete::class, "sample") { SampleDiscrete.serializer() },
    )
}

internal val regressionStatSpecNodes: List<WireNode> by lazy {
    listOf(
        WireNode(BayesianRegression::class, "bayesianRegression") { BayesianRegression.serializer() },
        WireNode(StochasticRegression::class, "stochasticRegression") { StochasticRegression.serializer() },
        WireNode(GaussianNaiveBayes::class, "gaussianNaiveBayes") { GaussianNaiveBayes.serializer() },
        WireNode(SoftmaxRegression::class, "softmaxRegression") { SoftmaxRegression.serializer() },
        WireNode(DiagonalRegression::class, "diagonalRegression") { DiagonalRegression.serializer() },
        WireNode(DecisionTreeRegression::class, "decisionTreeRegression") { DecisionTreeRegression.serializer() },
        WireNode(RandomForestRegression::class, "randomForestRegression") { RandomForestRegression.serializer() },
        WireNode(DecisionTreeClassifier::class, "decisionTreeClassifier") { DecisionTreeClassifier.serializer() },
        WireNode(RandomForestClassifier::class, "randomForestClassifier") { RandomForestClassifier.serializer() },
        WireNode(StandardScalerRegression::class, "standardScaler") { StandardScalerRegression.serializer() },
        WireNode(MinMaxScalerRegression::class, "minMaxScaler") { MinMaxScalerRegression.serializer() },
        WireNode(FilterRegression::class, "filter", WireShape.Filter) { FilterRegression.serializer() },
        WireNode(TransformYRegression::class, "transformY") { TransformYRegression.serializer() },
        WireNode(TransformXRegression::class, "transformX") { TransformXRegression.serializer() },
        WireNode(WithWeightRegression::class, "withWeight") { WithWeightRegression.serializer() },
        WireNode(WeightByValueRegression::class, "weightBy") { WeightByValueRegression.serializer() },
        WireNode(ThrottleRegression::class, "throttle") { ThrottleRegression.serializer() },
        WireNode(SampleRegression::class, "sample") { SampleRegression.serializer() },
        WireNode(FoldRegression::class, "foldRegression") { FoldRegression.serializer() },
    )
}

internal val statSpecNodes: List<WireNode> by lazy {
    seriesStatSpecNodes +
        pairedStatSpecNodes +
        vectorStatSpecNodes +
        discreteStatSpecNodes +
        regressionStatSpecNodes
}

internal object StatSpecSerializer : CanonicalSerializer<StatSpec>(StatSpec::class, { statSpecNodes })

internal val statModalities: Map<kotlin.reflect.KClass<*>, String> by lazy {
    listOf(
        "series" to seriesStatSpecNodes,
        "paired" to pairedStatSpecNodes,
        "vector" to vectorStatSpecNodes,
        "discrete" to discreteStatSpecNodes,
        "regression" to regressionStatSpecNodes,
    ).flatMap { (modality, nodes) -> nodes.map { it.type to modality } }.toMap()
}
