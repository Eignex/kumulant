package com.eignex.kumulant.schema.serialization

import com.eignex.kumulant.schema.spec.*
import com.eignex.skema.serialization.jsonNode

internal object SeriesStatSpecSerializer : KumulantSerializer<SeriesStatSpec<*>>(SeriesStatSpec::class, {
    listOf(
        jsonNode<Mean>("mean"),
        jsonNode<Sum>("sum"),
        jsonNode<Min>("min"),
        jsonNode<Max>("max"),
        jsonNode<ArgMin>("argMin"),
        jsonNode<ArgMax>("argMax"),
        jsonNode<Range>("range"),
        jsonNode<Excursion>("excursion"),
        jsonNode<RunLength>("runLength"),
        jsonNode<Recency>("recency"),
        jsonNode<Crossing>("crossing"),
        jsonNode<Cusum>("cusum"),
        jsonNode<PageHinkley>("pageHinkley"),
        jsonNode<Adwin>("adwin"),
        jsonNode<Mad>("mad"),
        jsonNode<FrugalMad>("frugalMad"),
        jsonNode<ThresholdBucket>("thresholdBucket"),
        jsonNode<Variance>("variance"),
        jsonNode<Moments>("moments"),
        jsonNode<ClassCounts>("classCounts"),
        jsonNode<Summary>("summary"),
        jsonNode<BernoulliSum>("bernoulliSum"),
        jsonNode<TotalWeights>("totalWeights"),
        jsonNode<Count>("count"),
        jsonNode<Rate>("rate"),
        jsonNode<CounterRate>("counterRate"),
        jsonNode<DDSketch>("ddSketch"),
        jsonNode<FrugalQuantile>("frugalQuantile"),
        jsonNode<HdrHistogram>("hdrHistogram"),
        jsonNode<LinearHistogram>("linearHistogram"),
        jsonNode<ReservoirHistogram>("reservoirHistogram"),
        jsonNode<TDigest>("tDigest"),
        jsonNode<PitHistogram>("pitHistogram"),
        jsonNode<WithWeightSeries>("withWeight"),
        jsonNode<WithValueSeries>("withValue"),
        jsonNode<AsSeries>("asSeries"),
        jsonNode<WithFixedX>("withFixedX"),
        jsonNode<WithFixedY>("withFixedY"),
        jsonNode<WithTimeAsX>("withTimeAsX"),
        jsonNode<WithTimeAsY>("withTimeAsY"),
        jsonNode<WindowedSeries>("window"),
        jsonNode<TransformValueSeries>("transform"),
        jsonNode<FilterValueSeries>("filter"),
        jsonNode<WeightByValueSeries>("weightBy"),
        jsonNode<ThrottleSeries>("throttle"),
        jsonNode<LagSeries>("lag"),
        jsonNode<DiffSeries>("diff"),
        jsonNode<DerivativeSeries>("derivative"),
        jsonNode<HysteresisSeries>("hysteresis"),
        jsonNode<ResampleByTimeSeries>("resampleByTime"),
        jsonNode<BandSeries>("band"),
        jsonNode<WithSelfLagSeries>("selfLag"),
        jsonNode<WithFeedbackSeries>("feedback"),
        jsonNode<StandardScalerSeries>("standardScaler"),
        jsonNode<MinMaxScalerSeries>("minMaxScaler"),
        jsonNode<SampleSeries>("sample"),
        jsonNode<GroupStatSpec>("group"),
        jsonNode<DecayingSum>("decayingSum"),
        jsonNode<DecayingMean>("decayingMean"),
        jsonNode<DecayingVariance>("decayingVariance"),
        jsonNode<EwmaMean>("ewma"),
        jsonNode<EwmaVariance>("ewmaVariance"),
        jsonNode<Holt>("holt"),
        jsonNode<SeasonalSmoothing>("seasonalSmoothing"),
        jsonNode<RecursiveVariance>("recursiveVariance"),
        jsonNode<DecayingRate>("decayingRate"),
    )
})

internal object PairedStatSpecSerializer : KumulantSerializer<PairedStatSpec<*>>(PairedStatSpec::class, {
    listOf(
        jsonNode<PairedGroupStatSpec>("pairedGroup"),
        jsonNode<PairedSum>("pairedSum"),
        jsonNode<UnivariateRegression>("univariateRegression"),
        jsonNode<Covariance>("covariance"),
        jsonNode<MseLoss>("mseLoss"),
        jsonNode<MaeLoss>("maeLoss"),
        jsonNode<LogLoss>("logLoss"),
        jsonNode<PinballLoss>("pinballLoss"),
        jsonNode<Auc>("auc"),
        jsonNode<Reliability>("reliability"),
        jsonNode<PlattCalibrator>("plattCalibrator"),
        jsonNode<ConfusionMatrix>("confusionMatrix"),
        jsonNode<Accuracy>("accuracy"),
        jsonNode<WithWeightPaired>("withWeight"),
        jsonNode<AtX>("atX"),
        jsonNode<AtY>("atY"),
        jsonNode<WindowedPaired>("window"),
        jsonNode<TransformPair>("transformPair"),
        jsonNode<FilterPaired>("filter"),
        jsonNode<FoldPaired>("foldPaired"),
        jsonNode<WeightByValuePaired>("weightBy"),
        jsonNode<ThrottlePaired>("throttle"),
        jsonNode<WithFeedbackPaired>("feedback"),
        jsonNode<StandardScalerPaired>("standardScaler"),
        jsonNode<MinMaxScalerPaired>("minMaxScaler"),
        jsonNode<SamplePaired>("sample"),
    )
})

internal object VectorStatSpecSerializer : KumulantSerializer<VectorStatSpec<*>>(VectorStatSpec::class, {
    listOf(
        jsonNode<VectorGroupStatSpec>("vectorGroup"),
        jsonNode<WithWeightVector>("withWeight"),
        jsonNode<AtIndex>("atIndex"),
        jsonNode<AtIndices>("atIndices"),
        jsonNode<WindowedVector>("window"),
        jsonNode<Vectorized>("vectorize"),
        jsonNode<TransformVectorElement>("transformElements"),
        jsonNode<FilterVector>("filter"),
        jsonNode<FoldVector>("foldVector"),
        jsonNode<FoldVectorPaired>("foldVectorPaired"),
        jsonNode<TransformVector>("transformVector"),
        jsonNode<WeightByValueVector>("weightBy"),
        jsonNode<ThrottleVector>("throttle"),
        jsonNode<WithFeedbackVector>("feedback"),
        jsonNode<StandardScalerVector>("standardScaler"),
        jsonNode<MinMaxScalerVector>("minMaxScaler"),
        jsonNode<SampleVector>("sample"),
    )
})

internal object DiscreteStatSpecSerializer : KumulantSerializer<DiscreteStatSpec<*>>(DiscreteStatSpec::class, {
    listOf(
        jsonNode<DiscreteGroupStatSpec>("discreteGroup"),
        jsonNode<HyperLogLog>("hyperLogLog"),
        jsonNode<LinearCounting>("linearCounting"),
        jsonNode<BloomFilter>("bloomFilter"),
        jsonNode<CountMinSketch>("countMinSketch"),
        jsonNode<MinHash>("minHash"),
        jsonNode<Sojourn>("sojourn"),
        jsonNode<SpaceSaving>("spaceSaving"),
        jsonNode<WithWeightDiscrete>("withWeight"),
        jsonNode<WithValueDiscrete>("withValue"),
        jsonNode<AsDiscrete>("asDiscrete"),
        jsonNode<WindowedDiscrete>("window"),
        jsonNode<TransformValueDiscrete>("transform"),
        jsonNode<FilterValueDiscrete>("filter"),
        jsonNode<WeightByValueDiscrete>("weightBy"),
        jsonNode<ThrottleDiscrete>("throttle"),
        jsonNode<SampleDiscrete>("sample"),
    )
})

internal object RegressionStatSpecSerializer : KumulantSerializer<RegressionStatSpec<*>>(RegressionStatSpec::class, {
    listOf(
        jsonNode<RegressionGroupStatSpec>("regressionGroup"),
        jsonNode<BayesianRegression>("bayesianRegression"),
        jsonNode<StochasticRegression>("stochasticRegression"),
        jsonNode<GaussianNaiveBayes>("gaussianNaiveBayes"),
        jsonNode<SoftmaxRegression>("softmaxRegression"),
        jsonNode<DiagonalRegression>("diagonalRegression"),
        jsonNode<DecisionTreeRegression>("decisionTreeRegression"),
        jsonNode<RandomForestRegression>("randomForestRegression"),
        jsonNode<DecisionTreeClassifier>("decisionTreeClassifier"),
        jsonNode<RandomForestClassifier>("randomForestClassifier"),
        jsonNode<WithFeedbackRegression>("feedback"),
        jsonNode<StandardScalerRegression>("standardScaler"),
        jsonNode<MinMaxScalerRegression>("minMaxScaler"),
        jsonNode<FilterRegression>("filter"),
        jsonNode<TransformYRegression>("transformY"),
        jsonNode<TransformXRegression>("transformX"),
        jsonNode<WithWeightRegression>("withWeight"),
        jsonNode<WeightByValueRegression>("weightBy"),
        jsonNode<ThrottleRegression>("throttle"),
        jsonNode<SampleRegression>("sample"),
        jsonNode<FoldRegression>("foldRegression"),
    )
})

internal val statSerializers: List<KumulantSerializer<*>> by lazy {
    listOf(
        SeriesStatSpecSerializer,
        PairedStatSpecSerializer,
        VectorStatSpecSerializer,
        DiscreteStatSpecSerializer,
        RegressionStatSpecSerializer,
    )
}

internal object StatSpecSerializer : KumulantSerializer<StatSpec>(
    StatSpec::class,
    { statSerializers.flatMap { it.nodes } },
)

internal val statModalities by lazy {
    statSerializers.flatMap { root -> root.nodes.map { it.type to root.serialName } }.toMap()
}
