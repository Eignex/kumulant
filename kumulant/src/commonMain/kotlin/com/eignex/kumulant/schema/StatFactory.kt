@file:Suppress("UNCHECKED_CAST")

package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.HasCenterScale
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.operation.FilterDiscreteStat
import com.eignex.kumulant.operation.FilterPairedStat
import com.eignex.kumulant.operation.FilterSeriesStat
import com.eignex.kumulant.operation.FilterVectorStat
import com.eignex.kumulant.operation.FoldPairedStat
import com.eignex.kumulant.operation.FoldVectorPairedStat
import com.eignex.kumulant.operation.FoldVectorStat
import com.eignex.kumulant.operation.TransformLongStat
import com.eignex.kumulant.operation.TransformPairStat
import com.eignex.kumulant.operation.TransformValueStat
import com.eignex.kumulant.operation.TransformVectorStat
import com.eignex.kumulant.operation.VectorizedStat
import com.eignex.kumulant.operation.asDiscrete
import com.eignex.kumulant.operation.asSeries
import com.eignex.kumulant.operation.atIndex
import com.eignex.kumulant.operation.atIndices
import com.eignex.kumulant.operation.atX
import com.eignex.kumulant.operation.atY
import com.eignex.kumulant.operation.band
import com.eignex.kumulant.operation.derivative
import com.eignex.kumulant.operation.diff
import com.eignex.kumulant.operation.filter
import com.eignex.kumulant.operation.foldRegression
import com.eignex.kumulant.operation.hysteresis
import com.eignex.kumulant.operation.lag
import com.eignex.kumulant.operation.minMaxScaleFeatures
import com.eignex.kumulant.operation.minMaxScaler
import com.eignex.kumulant.operation.resampleByTime
import com.eignex.kumulant.operation.sample
import com.eignex.kumulant.operation.standardScaleFeatures
import com.eignex.kumulant.operation.standardScaler
import com.eignex.kumulant.operation.throttle
import com.eignex.kumulant.operation.transformX
import com.eignex.kumulant.operation.transformY
import com.eignex.kumulant.operation.weightBy
import com.eignex.kumulant.operation.windowed
import com.eignex.kumulant.operation.withFeedback
import com.eignex.kumulant.operation.withFixedX
import com.eignex.kumulant.operation.withFixedY
import com.eignex.kumulant.operation.withSelfLag
import com.eignex.kumulant.operation.withTimeAsX
import com.eignex.kumulant.operation.withTimeAsY
import com.eignex.kumulant.operation.withValue
import com.eignex.kumulant.operation.withWeight
import com.eignex.kumulant.stat.cardinality.HyperLogLogStat
import com.eignex.kumulant.stat.cardinality.LinearCountingStat
import com.eignex.kumulant.stat.change.AdwinStat
import com.eignex.kumulant.stat.change.CusumStat
import com.eignex.kumulant.stat.change.PageHinkleyStat
import com.eignex.kumulant.stat.decay.DecayingMeanStat
import com.eignex.kumulant.stat.decay.DecayingSumStat
import com.eignex.kumulant.stat.decay.DecayingVarianceStat
import com.eignex.kumulant.stat.decay.EwmaMeanStat
import com.eignex.kumulant.stat.decay.EwmaVarianceStat
import com.eignex.kumulant.stat.event.CrossingStat
import com.eignex.kumulant.stat.event.ExcursionStat
import com.eignex.kumulant.stat.event.RecencyStat
import com.eignex.kumulant.stat.event.RunLengthStat
import com.eignex.kumulant.stat.event.SojournStat
import com.eignex.kumulant.stat.forecast.HoltStat
import com.eignex.kumulant.stat.forecast.RecursiveVarianceStat
import com.eignex.kumulant.stat.forecast.SeasonalSmoothingStat
import com.eignex.kumulant.stat.quantile.DDSketchStat
import com.eignex.kumulant.stat.quantile.FrugalQuantileStat
import com.eignex.kumulant.stat.quantile.HdrHistogramStat
import com.eignex.kumulant.stat.quantile.LinearHistogramStat
import com.eignex.kumulant.stat.quantile.ReservoirHistogramStat
import com.eignex.kumulant.stat.quantile.TDigestStat
import com.eignex.kumulant.stat.quantile.ThresholdBucketStat
import com.eignex.kumulant.stat.rate.CounterRateStat
import com.eignex.kumulant.stat.rate.DecayingRateStat
import com.eignex.kumulant.stat.rate.RateStat
import com.eignex.kumulant.stat.regression.CovarianceStat
import com.eignex.kumulant.stat.regression.GaussianNaiveBayesStat
import com.eignex.kumulant.stat.regression.SoftmaxRegressionStat
import com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat
import com.eignex.kumulant.stat.regression.glm.DiagonalRegressionStat
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat
import com.eignex.kumulant.stat.regression.glm.UnivariateRegressionStat
import com.eignex.kumulant.stat.regression.tree.DecisionTreeClassifierStat
import com.eignex.kumulant.stat.regression.tree.DecisionTreeRegressionStat
import com.eignex.kumulant.stat.regression.tree.RandomForestClassifierStat
import com.eignex.kumulant.stat.regression.tree.RandomForestRegressionStat
import com.eignex.kumulant.stat.score.AccuracyStat
import com.eignex.kumulant.stat.score.AucStat
import com.eignex.kumulant.stat.score.BrierScoreStat
import com.eignex.kumulant.stat.score.ConfusionMatrixStat
import com.eignex.kumulant.stat.score.LogLossStat
import com.eignex.kumulant.stat.score.MaeLossStat
import com.eignex.kumulant.stat.score.MseLossStat
import com.eignex.kumulant.stat.score.PinballLossStat
import com.eignex.kumulant.stat.score.ReliabilityStat
import com.eignex.kumulant.stat.score.pitHistogram
import com.eignex.kumulant.stat.sketch.BloomFilterStat
import com.eignex.kumulant.stat.sketch.CountMinSketchStat
import com.eignex.kumulant.stat.sketch.MinHashStat
import com.eignex.kumulant.stat.sketch.SpaceSavingStat
import com.eignex.kumulant.stat.summary.BernoulliSumStat
import com.eignex.kumulant.stat.summary.CountStat
import com.eignex.kumulant.stat.summary.MadStat
import com.eignex.kumulant.stat.summary.MaxStat
import com.eignex.kumulant.stat.summary.MeanStat
import com.eignex.kumulant.stat.summary.MinStat
import com.eignex.kumulant.stat.summary.MomentsStat
import com.eignex.kumulant.stat.summary.PairedSumStat
import com.eignex.kumulant.stat.summary.RangeStat
import com.eignex.kumulant.stat.summary.SumStat
import com.eignex.kumulant.stat.summary.SummaryStat
import com.eignex.kumulant.stat.summary.TotalWeightsStat
import com.eignex.kumulant.stat.summary.VarianceStat
import kotlin.random.Random
import kotlin.time.Duration.Companion.milliseconds

/**
 * Construct a live [SeriesStat] from a [SeriesStatSpec]. One `when` per modality,
 * one cast at the boundary - sealed-hierarchy exhaustiveness keeps the cast safe.
 *
 * The wrapper-spec branches narrow the wrapped `inner` spec to the expected modality
 * at runtime; on mismatch they raise the same `IllegalArgumentException` the previous
 * per-spec `materialize` overrides did.
 */
fun <R : Result> SeriesStatSpec<R>.materialize(concurrency: Concurrency = Concurrency.None): SeriesStat<R> {
    val out: SeriesStat<*> = when (this) {
        Mean -> MeanStat(concurrency)

        Sum -> SumStat(concurrency)

        Min -> MinStat(concurrency)

        Max -> MaxStat(concurrency)

        Range -> RangeStat(concurrency)

        Excursion -> ExcursionStat(concurrency)

        RunLength -> RunLengthStat(concurrency)

        Recency -> RecencyStat(concurrency)

        is Crossing -> CrossingStat(level, concurrency)

        is ThresholdBucket -> ThresholdBucketStat(thresholds.toDoubleArray(), concurrency)

        is Mad -> MadStat(compression, concurrency)

        is Cusum -> CusumStat(target, referenceValue, threshold, concurrency)

        is PageHinkley -> PageHinkleyStat(delta, threshold, concurrency)

        is Adwin -> AdwinStat(delta, maxBucketsPerSize, concurrency)

        Variance -> VarianceStat(concurrency)

        Moments -> MomentsStat(concurrency)

        Summary -> SummaryStat(concurrency)

        BernoulliSum -> BernoulliSumStat(concurrency)

        TotalWeights -> TotalWeightsStat(concurrency)

        Count -> CountStat(concurrency)

        Rate -> RateStat(concurrency)

        is CounterRate -> CounterRateStat(concurrency, treatDecreaseAsReset)

        is DDSketch -> DDSketchStat(relativeError, probabilities.toDoubleArray(), concurrency)

        is FrugalQuantile -> FrugalQuantileStat(q, stepSize, initialEstimate, concurrency)

        is HdrHistogram -> HdrHistogramStat(
            lowestDiscernibleValue,
            initialHighestTrackableValue,
            significantDigits,
            concurrency,
        )

        is LinearHistogram -> LinearHistogramStat(lowerBound, upperBound, binCount, concurrency)

        is ReservoirHistogram -> ReservoirHistogramStat(capacity, seed, concurrency)

        is TDigest -> TDigestStat(compression, probabilities.toDoubleArray(), concurrency)

        is PitHistogram -> pitHistogram(numBins, concurrency)

        is DecayingSum -> DecayingSumStat(weighting.toDecayWeighting(), concurrency)

        is DecayingMean -> DecayingMeanStat(weighting.toDecayWeighting(), concurrency)

        is DecayingVariance -> DecayingVarianceStat(weighting.toDecayWeighting(), concurrency)

        is EwmaMean -> EwmaMeanStat(weighting.toDecayWeighting(), concurrency)

        is EwmaVariance -> EwmaVarianceStat(weighting.toDecayWeighting(), concurrency)

        is Holt -> HoltStat(
            alphaWeighting = alphaWeighting.toDecayWeighting(),
            betaWeighting = betaWeighting.toDecayWeighting(),
            phi = phi,
            concurrency = concurrency,
        )

        is SeasonalSmoothing -> SeasonalSmoothingStat(
            alphaWeighting = alphaWeighting.toDecayWeighting(),
            betaWeighting = betaWeighting.toDecayWeighting(),
            gammaWeighting = gammaWeighting.toDecayWeighting(),
            period = period,
            mode = mode,
            phi = phi,
            concurrency = concurrency,
        )

        is RecursiveVariance -> RecursiveVarianceStat(omega, alpha, beta, concurrency)

        is DecayingRate -> DecayingRateStat(halfLifeMillis.milliseconds, concurrency)

        is GroupStatSpec -> {
            val children = stats.map { (name, config) ->
                require(config is SeriesStatSpec<*>) {
                    "GroupStatSpec entry '$name' has config ${config::class.simpleName}, expected a SeriesStatSpec"
                }
                toSpec(StatKey<Result>(name), config.materialize(concurrency))
            }
            StatGroup(stats = children, concurrency = concurrency)
        }

        is WithWeightSeries ->
            requireSeries(inner, "WithWeightSeries").materialize(concurrency).withWeight(weight)

        is WithValueSeries ->
            requireSeries(inner, "WithValueSeries").materialize(concurrency).withValue(value)

        is AsSeries ->
            requireDiscrete(inner, "AsSeries").materialize(concurrency).asSeries()

        is WithFixedX ->
            requirePaired(inner, "WithFixedX").materialize(concurrency).withFixedX(fixedX)

        is WithFixedY ->
            requirePaired(inner, "WithFixedY").materialize(concurrency).withFixedY(fixedY)

        is WithTimeAsX ->
            requirePaired(inner, "WithTimeAsX").materialize(concurrency).withTimeAsX()

        is WithTimeAsY ->
            requirePaired(inner, "WithTimeAsY").materialize(concurrency).withTimeAsY()

        is WindowedSeries ->
            requireSeries(inner, "WindowedSeries").materialize(concurrency)
                .windowed(durationMillis.milliseconds, slices, concurrency)

        is TransformValueSeries -> {
            val m = requireSeries(inner, "TransformValueSeries").materialize(concurrency) as SeriesStat<Result>
            TransformValueStat(m) { expr.eval(it) }
        }

        is FilterValueSeries -> {
            val m = requireSeries(inner, "FilterValueSeries").materialize(concurrency) as SeriesStat<Result>
            FilterSeriesStat(m) { pred.eval(it) }
        }

        is WeightByValueSeries ->
            (requireSeries(inner, "WeightByValueSeries").materialize(concurrency) as SeriesStat<Result>)
                .weightBy { v -> expr.eval(v) }

        is ThrottleSeries ->
            requireSeries(inner, "ThrottleSeries").materialize(concurrency).throttle(every)

        is SampleSeries ->
            requireSeries(inner, "SampleSeries").materialize(concurrency).sample(rate, Random(seed))

        is LagSeries ->
            requireSeries(inner, "LagSeries").materialize(concurrency).lag(k)

        is DiffSeries ->
            requireSeries(inner, "DiffSeries").materialize(concurrency).diff(k)

        is DerivativeSeries ->
            requireSeries(inner, "DerivativeSeries").materialize(concurrency).derivative()

        is HysteresisSeries ->
            requireSeries(inner, "HysteresisSeries").materialize(concurrency).hysteresis(low, high)

        is ResampleByTimeSeries ->
            requireSeries(inner, "ResampleByTimeSeries").materialize(concurrency)
                .resampleByTime(bucketMillis.milliseconds, aggregator)

        is WithSelfLagSeries ->
            requirePaired(inner, "WithSelfLagSeries").materialize(concurrency).withSelfLag(k)

        is WithFeedbackSeries -> {
            val innerStat = requireSeries(inner, "WithFeedbackSeries").materialize(concurrency) as SeriesStat<Result>
            val primaryStat = requireSeries(
                primary,
                "WithFeedbackSeries",
            ).materialize(concurrency) as SeriesStat<Result>
            innerStat.withFeedback(primaryStat, project)
        }

        is StandardScalerSeries ->
            requireSeries(inner, "StandardScaler").materialize(concurrency).standardScaler(concurrency)

        is MinMaxScalerSeries ->
            requireSeries(inner, "MinMaxScaler").materialize(concurrency)
                .minMaxScaler(targetLow, targetHigh, concurrency)

        is BandSeries -> {
            // The runtime check happens at the first read; the cast is safe iff the inner stat's
            // Result implements HasCenterScale, which is part of BandSeries's documented contract.
            @Suppress("UNCHECKED_CAST")
            val centerScaleInner = requireSeries(inner, "BandSeries").materialize(concurrency)
                as SeriesStat<HasCenterScale>
            centerScaleInner.band(k)
        }
    }
    return out as SeriesStat<R>
}

/**
 * Construct a live [PairedStat] from a [PairedStatSpec]. See [SeriesStatSpec.materialize].
 */
fun <R : Result> PairedStatSpec<R>.materialize(concurrency: Concurrency = Concurrency.None): PairedStat<R> {
    val out: PairedStat<*> = when (this) {
        PairedSum -> PairedSumStat(concurrency)

        is UnivariateRegression -> UnivariateRegressionStat(penalty, concurrency)

        Covariance -> CovarianceStat(concurrency)

        BrierScore -> BrierScoreStat(concurrency)

        MseLoss -> MseLossStat(concurrency)

        MaeLoss -> MaeLossStat(concurrency)

        LogLoss -> LogLossStat(concurrency)

        is PinballLoss -> PinballLossStat(tau, concurrency)

        is Auc -> AucStat(numBins, lowerBound, upperBound, concurrency)

        is Reliability -> ReliabilityStat(numBins, concurrency)

        is ConfusionMatrix -> ConfusionMatrixStat(numClasses, concurrency)

        Accuracy -> AccuracyStat(concurrency)

        is WithWeightPaired ->
            requirePaired(inner, "WithWeightPaired").materialize(concurrency).withWeight(weight)

        is AtX ->
            requireSeries(inner, "AtX").materialize(concurrency).atX()

        is AtY ->
            requireSeries(inner, "AtY").materialize(concurrency).atY()

        is WindowedPaired ->
            requirePaired(inner, "WindowedPaired").materialize(concurrency)
                .windowed(durationMillis.milliseconds, slices, concurrency)

        is TransformPair -> {
            val m = requirePaired(inner, "TransformPair").materialize(concurrency) as PairedStat<Result>
            TransformPairStat(m) { xv, yv -> xExpr.eval(xv, yv) to yExpr.eval(xv, yv) }
        }

        is FilterPaired -> {
            val m = requirePaired(inner, "FilterPaired").materialize(concurrency) as PairedStat<Result>
            FilterPairedStat(m) { xv, yv -> pred.eval(xv, yv) }
        }

        is FoldPaired -> {
            val m = requireSeries(inner, "FoldPaired").materialize(concurrency) as SeriesStat<Result>
            FoldPairedStat(m) { xv, yv -> expr.eval(xv, yv) }
        }

        is WeightByValuePaired ->
            (requirePaired(inner, "WeightByValuePaired").materialize(concurrency) as PairedStat<Result>)
                .weightBy { xv, yv -> expr.eval(xv, yv) }

        is ThrottlePaired ->
            requirePaired(inner, "ThrottlePaired").materialize(concurrency).throttle(every)

        is SamplePaired ->
            requirePaired(inner, "SamplePaired").materialize(concurrency).sample(rate, Random(seed))

        is StandardScalerPaired ->
            requirePaired(inner, "StandardScalerPaired").materialize(concurrency).standardScaler(concurrency)

        is MinMaxScalerPaired ->
            requirePaired(inner, "MinMaxScalerPaired").materialize(concurrency)
                .minMaxScaler(targetLow, targetHigh, concurrency)
    }
    return out as PairedStat<R>
}

/**
 * Construct a live [VectorStat] from a [VectorStatSpec]. See [SeriesStatSpec.materialize].
 */
fun <R : Result> VectorStatSpec<R>.materialize(concurrency: Concurrency = Concurrency.None): VectorStat<R> {
    val out: VectorStat<*> = when (this) {
        is WithWeightVector ->
            requireVector(inner, "WithWeightVector").materialize(concurrency).withWeight(weight)

        is AtIndex ->
            requireSeries(inner, "AtIndex").materialize(concurrency).atIndex(index)

        is AtIndices ->
            requirePaired(inner, "AtIndices").materialize(concurrency).atIndices(indexX, indexY)

        is WindowedVector ->
            requireVector(inner, "WindowedVector").materialize(concurrency)
                .windowed(durationMillis.milliseconds, slices, concurrency)

        is Vectorized -> {
            val tpl = requireSeries(template, "Vectorized").materialize(concurrency) as SeriesStat<Result>
            VectorizedStat(dimensions, tpl, skipZeros)
        }

        is TransformVectorElement -> {
            val m = requireVector(inner, "TransformVectorElement").materialize(concurrency) as VectorStat<Result>
            TransformVectorStat(m) { vec -> DoubleArray(vec.size) { i -> expr.eval(vec[i], 0.0, vec) } }
        }

        is FilterVector -> {
            val m = requireVector(inner, "FilterVector").materialize(concurrency) as VectorStat<Result>
            FilterVectorStat(m) { vec -> pred.eval(0.0, 0.0, vec) }
        }

        is FoldVector -> {
            val m = requireSeries(inner, "FoldVector").materialize(concurrency) as SeriesStat<Result>
            FoldVectorStat(m) { vec -> expr.eval(0.0, 0.0, vec) }
        }

        is FoldVectorPaired -> {
            val m = requirePaired(inner, "FoldVectorPaired").materialize(concurrency) as PairedStat<Result>
            FoldVectorPairedStat(
                m,
                foldX = { vec -> xExpr.eval(0.0, 0.0, vec) },
                foldY = { vec -> yExpr.eval(0.0, 0.0, vec) },
            )
        }

        is TransformVector -> {
            val m = requireVector(inner, "TransformVector").materialize(concurrency) as VectorStat<Result>
            TransformVectorStat(m) { vec -> expr.eval(0.0, 0.0, vec) }
        }

        is WeightByValueVector ->
            (requireVector(inner, "WeightByValueVector").materialize(concurrency) as VectorStat<Result>)
                .weightBy { vec -> expr.eval(0.0, 0.0, vec) }

        is ThrottleVector ->
            requireVector(inner, "ThrottleVector").materialize(concurrency).throttle(every)

        is SampleVector ->
            requireVector(inner, "SampleVector").materialize(concurrency).sample(rate, Random(seed))

        is StandardScalerVector ->
            requireVector(inner, "StandardScalerVector").materialize(concurrency)
                .standardScaleFeatures(dimensions, concurrency)

        is MinMaxScalerVector ->
            requireVector(inner, "MinMaxScalerVector").materialize(concurrency)
                .minMaxScaleFeatures(dimensions, targetLow, targetHigh, concurrency)
    }
    return out as VectorStat<R>
}

/**
 * Construct a live [DiscreteStat] from a [DiscreteStatSpec]. See [SeriesStatSpec.materialize].
 */
fun <R : Result> DiscreteStatSpec<R>.materialize(concurrency: Concurrency = Concurrency.None): DiscreteStat<R> {
    val out: DiscreteStat<*> = when (this) {
        is HyperLogLog -> HyperLogLogStat(precision, concurrency)

        is LinearCounting -> LinearCountingStat(bits, concurrency)

        is BloomFilter -> BloomFilterStat(bits, hashes, concurrency)

        is CountMinSketch -> CountMinSketchStat(depth, width, seed, concurrency)

        is MinHash -> MinHashStat(numHashes, seed, concurrency)

        is SpaceSaving -> SpaceSavingStat(capacity, concurrency)

        is Sojourn -> SojournStat(states, concurrency)

        is WithWeightDiscrete ->
            requireDiscrete(inner, "WithWeightDiscrete").materialize(concurrency).withWeight(weight)

        is WithValueDiscrete ->
            requireDiscrete(inner, "WithValueDiscrete").materialize(concurrency).withValue(value)

        is AsDiscrete ->
            requireSeries(inner, "AsDiscrete").materialize(concurrency).asDiscrete()

        is WindowedDiscrete ->
            requireDiscrete(inner, "WindowedDiscrete").materialize(concurrency)
                .windowed(durationMillis.milliseconds, slices, concurrency)

        is TransformValueDiscrete -> {
            val m = requireDiscrete(inner, "TransformValueDiscrete").materialize(concurrency) as DiscreteStat<Result>
            TransformLongStat(m) { expr.eval(it.toDouble()).toLong() }
        }

        is FilterValueDiscrete -> {
            val m = requireDiscrete(inner, "FilterValueDiscrete").materialize(concurrency) as DiscreteStat<Result>
            FilterDiscreteStat(m) { pred.eval(it.toDouble()) }
        }

        is WeightByValueDiscrete ->
            (requireDiscrete(inner, "WeightByValueDiscrete").materialize(concurrency) as DiscreteStat<Result>)
                .weightBy { v -> expr.eval(v.toDouble()) }

        is ThrottleDiscrete ->
            requireDiscrete(inner, "ThrottleDiscrete").materialize(concurrency).throttle(every)

        is SampleDiscrete ->
            requireDiscrete(inner, "SampleDiscrete").materialize(concurrency).sample(rate, Random(seed))
    }
    return out as DiscreteStat<R>
}

/**
 * Construct a live stat from any [StatSpec], dispatching on its modality.
 * Useful for code paths (like [StatSchemaDef.materialize]) that iterate over
 * an erased `Map<String, StatSpec>` and don't statically know the modality.
 */
fun StatSpec.materialize(concurrency: Concurrency = Concurrency.None): Stat<*> = when (this) {
    is SeriesStatSpec<*> -> materialize(concurrency)
    is PairedStatSpec<*> -> materialize(concurrency)
    is VectorStatSpec<*> -> materialize(concurrency)
    is DiscreteStatSpec<*> -> materialize(concurrency)
    is RegressionStatSpec<*> -> materialize(concurrency)
}

private fun requireRegression(inner: StatSpec, op: String): RegressionStatSpec<*> {
    require(inner is RegressionStatSpec<*>) {
        "$op expects a RegressionStatSpec inner; got ${inner::class.simpleName}"
    }
    return inner
}

/**
 * Construct a live [RegressionStat] from a [RegressionStatSpec]. See [SeriesStatSpec.materialize].
 */
fun <R : Result> RegressionStatSpec<R>.materialize(concurrency: Concurrency = Concurrency.None): RegressionStat<R> {
    val out: RegressionStat<*> = when (this) {
        is BayesianRegression ->
            BayesianRegressionStat(featureSize, priorVariance, link, concurrency)

        is StochasticRegression ->
            StochasticRegressionStat(featureSize, optimizer, biasOptimizer, penalty, link, concurrency)

        is DiagonalRegression ->
            DiagonalRegressionStat(featureSize, priorPrecision, learningRate, penalty, link, concurrency)

        is SoftmaxRegression ->
            SoftmaxRegressionStat(featureSize, numClasses, optimizer, biasOptimizer, concurrency)

        is GaussianNaiveBayes ->
            GaussianNaiveBayesStat(featureSize, numClasses, varianceFloor, concurrency)

        is DecisionTreeRegression ->
            DecisionTreeRegressionStat(
                featureSize = featureSize,
                splitCandidates = splitCandidates,
                config = config,
                concurrency = concurrency,
                randomSeed = randomSeed,
            )

        is RandomForestRegression ->
            RandomForestRegressionStat(
                featureSize = featureSize,
                splitCandidates = splitCandidates,
                nbrTrees = nbrTrees,
                config = config,
                bagging = bagging,
                concurrency = concurrency,
                randomSeed = randomSeed,
            )

        is DecisionTreeClassifier ->
            DecisionTreeClassifierStat(
                featureSize = featureSize,
                numClasses = numClasses,
                splitCandidates = splitCandidates,
                config = config,
                concurrency = concurrency,
                randomSeed = randomSeed,
            )

        is RandomForestClassifier ->
            RandomForestClassifierStat(
                featureSize = featureSize,
                numClasses = numClasses,
                splitCandidates = splitCandidates,
                nbrTrees = nbrTrees,
                config = config,
                bagging = bagging,
                concurrency = concurrency,
                randomSeed = randomSeed,
            )

        is FilterRegression -> {
            val m = requireRegression(inner, "FilterRegression").materialize(concurrency) as RegressionStat<Result>
            m.filter { v, y -> pred.eval(0.0, y, v.toDoubleArray()) }
        }

        is TransformYRegression -> {
            val m = requireRegression(inner, "TransformYRegression").materialize(concurrency) as RegressionStat<Result>
            m.transformY { v, y -> expr.eval(0.0, y, v.toDoubleArray()) }
        }

        is TransformXRegression -> {
            val m = requireRegression(inner, "TransformXRegression").materialize(concurrency) as RegressionStat<Result>
            m.transformX { v, y -> expr.eval(0.0, y, v.toDoubleArray()) }
        }

        is WithWeightRegression ->
            requireRegression(inner, "WithWeightRegression").materialize(concurrency).withWeight(weight)

        is WeightByValueRegression -> {
            val m = requireRegression(
                inner,
                "WeightByValueRegression",
            ).materialize(concurrency) as RegressionStat<Result>
            m.weightBy { v, y -> expr.eval(0.0, y, v.toDoubleArray()) }
        }

        is ThrottleRegression ->
            requireRegression(inner, "ThrottleRegression").materialize(concurrency).throttle(every)

        is SampleRegression ->
            requireRegression(
                inner,
                "SampleRegression",
            ).materialize(concurrency).sample(rate, Random(seed))

        is FoldRegression -> {
            val m = requireSeries(inner, "FoldRegression").materialize(concurrency) as SeriesStat<Result>
            m.foldRegression(featureSize) { v, y -> project.eval(0.0, y, v.toDoubleArray()) }
        }

        is StandardScalerRegression ->
            requireRegression(inner, "StandardScalerRegression").materialize(concurrency)
                .standardScaleFeatures(concurrency)

        is MinMaxScalerRegression ->
            requireRegression(inner, "MinMaxScalerRegression").materialize(concurrency)
                .minMaxScaleFeatures(targetLow, targetHigh, concurrency)
    }
    return out as RegressionStat<R>
}

private fun requireSeries(inner: StatSpec, op: String): SeriesStatSpec<*> {
    require(inner is SeriesStatSpec<*>) {
        "$op expects a SeriesStatSpec inner; got ${inner::class.simpleName}"
    }
    return inner
}

private fun requirePaired(inner: StatSpec, op: String): PairedStatSpec<*> {
    require(inner is PairedStatSpec<*>) {
        "$op expects a PairedStatSpec inner; got ${inner::class.simpleName}"
    }
    return inner
}

private fun requireVector(inner: StatSpec, op: String): VectorStatSpec<*> {
    require(inner is VectorStatSpec<*>) {
        "$op expects a VectorStatSpec inner; got ${inner::class.simpleName}"
    }
    return inner
}

private fun requireDiscrete(inner: StatSpec, op: String): DiscreteStatSpec<*> {
    require(inner is DiscreteStatSpec<*>) {
        "$op expects a DiscreteStatSpec inner; got ${inner::class.simpleName}"
    }
    return inner
}
