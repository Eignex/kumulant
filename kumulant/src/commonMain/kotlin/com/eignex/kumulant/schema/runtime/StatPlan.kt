package com.eignex.kumulant.schema.runtime

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.HasCenterScale
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.RegressionStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.math.Hashers
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
import com.eignex.kumulant.operation.WeightByVectorStat
import com.eignex.kumulant.operation.asDiscrete
import com.eignex.kumulant.operation.asSeries
import com.eignex.kumulant.operation.band
import com.eignex.kumulant.operation.derivative
import com.eignex.kumulant.operation.diff
import com.eignex.kumulant.operation.filter
import com.eignex.kumulant.operation.foldRegression
import com.eignex.kumulant.operation.hysteresis
import com.eignex.kumulant.operation.lag
import com.eignex.kumulant.operation.minMaxScaleFeatures
import com.eignex.kumulant.operation.resampleByTime
import com.eignex.kumulant.operation.sample
import com.eignex.kumulant.operation.standardScaleFeatures
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
import com.eignex.kumulant.schema.*
import com.eignex.kumulant.schema.expr.BoolExpr
import com.eignex.kumulant.schema.expr.ExprInputs
import com.eignex.kumulant.schema.expr.ExprInputs.Scope
import com.eignex.kumulant.schema.expr.ScalarExpr
import com.eignex.kumulant.schema.expr.VectorExpr
import com.eignex.kumulant.schema.expr.discretePredicate
import com.eignex.kumulant.schema.expr.eval
import com.eignex.kumulant.schema.expr.inputs
import com.eignex.kumulant.schema.expr.isDiscreteIdentity
import com.eignex.kumulant.schema.expr.requireBound
import com.eignex.kumulant.schema.spec.*
import com.eignex.kumulant.stat.calibration.PlattCalibratorStat
import com.eignex.kumulant.stat.calibration.ReliabilityStat
import com.eignex.kumulant.stat.cardinality.HyperLogLogStat
import com.eignex.kumulant.stat.change.AdwinStat
import com.eignex.kumulant.stat.change.CusumStat
import com.eignex.kumulant.stat.change.PageHinkleyStat
import com.eignex.kumulant.stat.decay.DecayWeighting
import com.eignex.kumulant.stat.decay.DecayingMeanStat
import com.eignex.kumulant.stat.decay.DecayingSumStat
import com.eignex.kumulant.stat.decay.DecayingVarianceStat
import com.eignex.kumulant.stat.decay.EwmaMeanStat
import com.eignex.kumulant.stat.decay.EwmaVarianceStat
import com.eignex.kumulant.stat.event.CrossingStat
import com.eignex.kumulant.stat.event.RecencyStat
import com.eignex.kumulant.stat.event.RunLengthStat
import com.eignex.kumulant.stat.event.SojournStat
import com.eignex.kumulant.stat.forecast.ExponentialSmoothingStat
import com.eignex.kumulant.stat.quantile.BucketHistogramStat
import com.eignex.kumulant.stat.quantile.DDSketchStat
import com.eignex.kumulant.stat.quantile.FrugalQuantileStat
import com.eignex.kumulant.stat.quantile.ReservoirHistogramStat
import com.eignex.kumulant.stat.quantile.TDigestStat
import com.eignex.kumulant.stat.rate.CounterRateStat
import com.eignex.kumulant.stat.rate.RateStat
import com.eignex.kumulant.stat.regression.CovarianceStat
import com.eignex.kumulant.stat.regression.GaussianNaiveBayesStat
import com.eignex.kumulant.stat.regression.SoftmaxRegressionStat
import com.eignex.kumulant.stat.regression.glm.BayesianRegressionStat
import com.eignex.kumulant.stat.regression.glm.DiagonalRegressionStat
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionStat
import com.eignex.kumulant.stat.regression.glm.UnivariateRegressionStat
import com.eignex.kumulant.stat.regression.tree.ClassCountsResult
import com.eignex.kumulant.stat.regression.tree.ClassCountsStat
import com.eignex.kumulant.stat.regression.tree.DecisionTreeClassifierStat
import com.eignex.kumulant.stat.regression.tree.DecisionTreeRegressionStat
import com.eignex.kumulant.stat.regression.tree.RandomForestClassifierStat
import com.eignex.kumulant.stat.regression.tree.RandomForestRegressionStat
import com.eignex.kumulant.stat.score.AccuracyStat
import com.eignex.kumulant.stat.score.AucStat
import com.eignex.kumulant.stat.score.ConfusionMatrixStat
import com.eignex.kumulant.stat.score.LogLossStat
import com.eignex.kumulant.stat.score.MaeLossStat
import com.eignex.kumulant.stat.score.MseLossStat
import com.eignex.kumulant.stat.score.PinballLossStat
import com.eignex.kumulant.stat.score.pitHistogram
import com.eignex.kumulant.stat.sketch.BloomFilterStat
import com.eignex.kumulant.stat.sketch.CountMinSketchStat
import com.eignex.kumulant.stat.sketch.SpaceSavingStat
import com.eignex.kumulant.stat.summary.ArgMaxStat
import com.eignex.kumulant.stat.summary.ArgMinStat
import com.eignex.kumulant.stat.summary.BernoulliSumStat
import com.eignex.kumulant.stat.summary.CountStat
import com.eignex.kumulant.stat.summary.FrugalMadStat
import com.eignex.kumulant.stat.summary.MadStat
import com.eignex.kumulant.stat.summary.MaxStat
import com.eignex.kumulant.stat.summary.MeanStat
import com.eignex.kumulant.stat.summary.MinStat
import com.eignex.kumulant.stat.summary.MomentsStat
import com.eignex.kumulant.stat.summary.PairedSumStat
import com.eignex.kumulant.stat.summary.RangeStat
import com.eignex.kumulant.stat.summary.SumStat
import com.eignex.kumulant.stat.summary.SummaryStat
import com.eignex.kumulant.stat.summary.VarianceStat
import kotlin.time.Duration.Companion.milliseconds

internal class StatPlan<out S : Stat<*>>(
    private val path: String,
    val hasCenterScale: Boolean = false,
    private val factory: (Concurrency) -> S,
) {
    fun build(concurrency: Concurrency): S = try {
        factory(concurrency)
    } catch (e: IllegalArgumentException) {
        throw IllegalArgumentException("$path: ${e.message}", e)
    }
}

internal fun StatSpec.compile(path: String): StatPlan<Stat<*>> = when (this) {
    is SeriesStatSpec<*> -> compileSeries(path)
    is PairedStatSpec<*> -> compilePaired(path)
    is VectorStatSpec<*> -> compileVector(path)
    is DiscreteStatSpec<*> -> compileDiscrete(path)
    is RegressionStatSpec<*> -> compileRegression(path)
}

internal fun SeriesStatSpec<*>.compileSeries(path: String): StatPlan<SeriesStat<*>> = when (this) {
    Mean -> StatPlan(path) { concurrency -> MeanStat(concurrency) }

    Sum -> StatPlan(path) { concurrency -> SumStat(concurrency) }

    Min -> StatPlan(path) { concurrency -> MinStat(concurrency) }

    Max -> StatPlan(path) { concurrency -> MaxStat(concurrency) }

    ArgMin -> StatPlan(path) { concurrency -> ArgMinStat(concurrency) }

    ArgMax -> StatPlan(path) { concurrency -> ArgMaxStat(concurrency) }

    Range -> StatPlan(path) { concurrency -> RangeStat(concurrency) }

    RunLength -> StatPlan(path) { concurrency -> RunLengthStat(concurrency) }

    Recency -> StatPlan(path) { concurrency -> RecencyStat(concurrency) }

    is Crossing -> StatPlan(path) { concurrency -> CrossingStat(level, concurrency) }

    is BucketHistogram ->
        StatPlan(path) { concurrency ->
            BucketHistogramStat(edges.toDoubleArray(), concurrency)
        }

    is Mad ->
        StatPlan(path, hasCenterScale = true) { concurrency ->
            MadStat(compression, concurrency)
        }

    is FrugalMad ->
        StatPlan(path, hasCenterScale = true) { concurrency -> FrugalMadStat(stepSize, concurrency) }

    is Cusum ->
        StatPlan(path) { concurrency ->
            CusumStat(target, referenceValue, threshold, concurrency)
        }

    is PageHinkley ->
        StatPlan(path) { concurrency -> PageHinkleyStat(delta, threshold, concurrency) }

    is Adwin ->
        StatPlan(path) { concurrency ->
            AdwinStat(delta, maxBucketsPerSize, concurrency)
        }

    Variance ->
        StatPlan(path, hasCenterScale = true) { concurrency -> VarianceStat(concurrency) }

    Moments -> StatPlan(path, hasCenterScale = true) { concurrency -> MomentsStat(concurrency) }

    Summary -> StatPlan(path, hasCenterScale = true) { concurrency -> SummaryStat(concurrency) }

    is ClassCounts -> StatPlan(path) { concurrency -> ClassCountsStat(numClasses, concurrency) }

    BernoulliSum -> StatPlan(path) { concurrency -> BernoulliSumStat(concurrency) }

    Count -> StatPlan(path) { concurrency -> CountStat(concurrency) }

    Rate -> StatPlan(path) { concurrency -> RateStat(concurrency) }

    is CounterRate ->
        StatPlan(path) { concurrency ->
            CounterRateStat(concurrency, treatDecreaseAsReset)
        }

    is DDSketch -> {
        StatPlan(path) { concurrency ->
            DDSketchStat(
                relativeError = relativeError,
                probabilities = probabilities.toDoubleArray(),
                minIndexableValue = minIndexableValue,
                maxIndexableValue = maxIndexableValue,
                concurrency = concurrency,
            )
        }
    }

    is FrugalQuantile ->
        StatPlan(path) { concurrency ->
            FrugalQuantileStat(q, stepSize, initialEstimate, concurrency)
        }

    is ReservoirHistogram ->
        StatPlan(path) { concurrency ->
            ReservoirHistogramStat(capacity, seed, concurrency)
        }

    is TDigest ->
        StatPlan(path) { concurrency ->
            TDigestStat(compression, probabilities.toDoubleArray(), concurrency)
        }

    is PitHistogram -> StatPlan(path) { concurrency -> pitHistogram(numBins, concurrency) }

    is DecayingSum ->
        StatPlan(path) { concurrency ->
            DecayingSumStat(DecayWeighting.HalfLife(halfLifeMillis.milliseconds), concurrency)
        }

    is DecayingMean ->
        StatPlan(path) { concurrency ->
            DecayingMeanStat(DecayWeighting.HalfLife(halfLifeMillis.milliseconds), concurrency)
        }

    is DecayingVariance ->
        StatPlan(path) { concurrency ->
            DecayingVarianceStat(DecayWeighting.HalfLife(halfLifeMillis.milliseconds), concurrency)
        }

    is EwmaMean ->
        StatPlan(path) { concurrency ->
            EwmaMeanStat(DecayWeighting.Alpha(alpha), concurrency)
        }

    is EwmaVariance ->
        StatPlan(
            path,
            true,
        ) { concurrency ->
            EwmaVarianceStat(DecayWeighting.Alpha(alpha), concurrency)
        }

    is ExponentialSmoothing -> {
        StatPlan(path) { concurrency ->
            ExponentialSmoothingStat(
                alphaWeighting = DecayWeighting.Alpha(alpha),
                betaWeighting = DecayWeighting.Alpha(beta),
                phi = phi,
                period = period,
                gammaWeighting = DecayWeighting.Alpha(gamma),
                mode = mode,
                concurrency = concurrency,
            )
        }
    }

    is GroupStatSpec -> {
        val children = stats.map { (name, config) ->
            name to requireSeries(config, "$path.$name").compileSeries("$path.$name")
        }
        StatPlan(path) { concurrency ->
            StatGroup(
                stats =
                children.map { (name, plan) ->
                    toSpec(StatKey<Result>(name), plan.build(concurrency))
                },
                concurrency = concurrency,
            )
        }
    }

    is WithWeightSeries -> {
        val innerPlan = requireSeries(inner, "$path.inner").compileSeries("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).withWeight(weight)
        }
    }

    is AsSeries -> {
        val innerPlan = requireDiscrete(inner, "$path.inner").compileDiscrete("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).asSeries()
        }
    }

    is WithFixedX -> {
        val innerPlan = requirePaired(inner, "$path.inner").compilePaired("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).withFixedX(fixedX)
        }
    }

    is WithFixedY -> {
        val innerPlan = requirePaired(inner, "$path.inner").compilePaired("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).withFixedY(fixedY)
        }
    }

    is WithTimeAsX -> {
        val innerPlan = requirePaired(inner, "$path.inner").compilePaired("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).withTimeAsX()
        }
    }

    is WithTimeAsY -> {
        val innerPlan = requirePaired(inner, "$path.inner").compilePaired("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).withTimeAsY()
        }
    }

    is WindowedSeries -> {
        val innerPlan = requireSeries(inner, "$path.inner").compileSeries("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan
                .build(concurrency)
                .windowed(durationMillis.milliseconds, slices, concurrency)
        }
    }

    is TransformValueSeries -> {
        expr.requireInputs("$path.expr", Scope.Scalar)
        val innerPlan = requireSeries(inner, "$path.inner").compileSeries("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            val m = innerPlan.build(concurrency)
            TransformValueStat(m) { expr.eval(it) }
        }
    }

    is FilterValueSeries -> {
        pred.requireInputs("$path.pred", Scope.Scalar)
        val innerPlan = requireSeries(inner, "$path.inner").compileSeries("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            val m = innerPlan.build(concurrency)
            FilterSeriesStat(m) { pred.eval(it) }
        }
    }

    is WeightByValueSeries -> {
        expr.requireInputs("$path.expr", Scope.Scalar)
        val innerPlan = requireSeries(inner, "$path.inner").compileSeries("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).weightBy { v -> expr.eval(v) }
        }
    }

    is ThrottleSeries -> {
        val innerPlan = requireSeries(inner, "$path.inner").compileSeries("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).throttle(every)
        }
    }

    is SampleSeries -> {
        val innerPlan = requireSeries(inner, "$path.inner").compileSeries("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).sample(rate, seed)
        }
    }

    is LagSeries -> {
        val innerPlan = requireSeries(inner, "$path.inner").compileSeries("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).lag(k)
        }
    }

    is DiffSeries -> {
        val innerPlan = requireSeries(inner, "$path.inner").compileSeries("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).diff(k)
        }
    }

    is DerivativeSeries -> {
        val innerPlan = requireSeries(inner, "$path.inner").compileSeries("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).derivative()
        }
    }

    is HysteresisSeries -> {
        val innerPlan = requireSeries(inner, "$path.inner").compileSeries("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).hysteresis(low, high)
        }
    }

    is ResampleByTimeSeries -> {
        val innerPlan = requireSeries(inner, "$path.inner").compileSeries("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).resampleByTime(bucketMillis.milliseconds, aggregator)
        }
    }

    is WithSelfLagSeries -> {
        val innerPlan = requirePaired(inner, "$path.inner").compilePaired("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).withSelfLag(k)
        }
    }

    is WithFeedbackSeries -> {
        val reads = project.requireInputs("$path.project", Scope.Feedback)
        val innerPlan = requireSeries(inner, "$path.inner").compileSeries("$path.inner")
        val primaryPlan = requireSeries(primary, "$path.primary").compileSeries("$path.primary")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            val innerStat = innerPlan.build(concurrency)
            val primaryStat = primaryPlan.build(concurrency)
            reads.requirePrimaries("$path.primary", primaryStat)
            innerStat.withFeedback(primaryStat, project)
        }
    }

    is BandSeries -> {
        val innerPlan = requireSeries(inner, "$path.inner").compileSeries("$path.inner")
        require(innerPlan.hasCenterScale) {
            "$path.inner: BandSeries requires an inner stat whose result has a center and scale, got $inner"
        }
        StatPlan(path) { concurrency ->
            @Suppress("UNCHECKED_CAST")
            (innerPlan.build(concurrency) as SeriesStat<HasCenterScale>).band(k)
        }
    }
}

@Suppress("UNCHECKED_CAST")
internal fun PairedStatSpec<*>.compilePaired(path: String): StatPlan<PairedStat<*>> = when (this) {
    is PairedGroupStatSpec -> {
        val plans = stats.map { (name, spec) ->
            name to requirePaired(spec, "$path.$name").compilePaired("$path.$name")
        }
        StatPlan(path) { concurrency ->
            PairedStatGroup(
                plans.map { (name, plan) ->
                    toSpec(StatKey<Result>(name), plan.build(concurrency))
                },
                concurrency,
            )
        }
    }

    PairedSum -> StatPlan(path) { concurrency -> PairedSumStat(concurrency) }

    is UnivariateRegression ->
        StatPlan(path) { concurrency ->
            UnivariateRegressionStat(penalty, concurrency)
        }

    Covariance -> StatPlan(path) { concurrency -> CovarianceStat(concurrency) }

    MseLoss -> StatPlan(path) { concurrency -> MseLossStat(concurrency) }

    MaeLoss -> StatPlan(path) { concurrency -> MaeLossStat(concurrency) }

    LogLoss -> StatPlan(path) { concurrency -> LogLossStat(concurrency) }

    is PinballLoss -> StatPlan(path) { concurrency -> PinballLossStat(tau, concurrency) }

    is Auc ->
        StatPlan(path) { concurrency ->
            AucStat(numBins, lowerBound, upperBound, concurrency)
        }

    is Reliability -> StatPlan(path) { concurrency -> ReliabilityStat(numBins, concurrency) }

    is PlattCalibrator ->
        StatPlan(path) { concurrency -> PlattCalibratorStat(optimizer, concurrency) }

    is ConfusionMatrix ->
        StatPlan(path) { concurrency -> ConfusionMatrixStat(numClasses, concurrency) }

    is Accuracy -> StatPlan(path) { concurrency -> AccuracyStat(numClasses, concurrency) }

    is WithWeightPaired -> {
        val innerPlan = requirePaired(inner, "$path.inner").compilePaired("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).withWeight(weight)
        }
    }

    is WindowedPaired -> {
        val innerPlan = requirePaired(inner, "$path.inner").compilePaired("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan
                .build(concurrency)
                .windowed(durationMillis.milliseconds, slices, concurrency)
        }
    }

    is TransformPair -> {
        xExpr.requireInputs("$path.xExpr", Scope.Scalar)
        yExpr.requireInputs("$path.yExpr", Scope.Scalar)
        val innerPlan = requirePaired(inner, "$path.inner").compilePaired("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            val m = innerPlan.build(concurrency)
            TransformPairStat(m) { xv, yv -> xExpr.eval(xv, yv) to yExpr.eval(xv, yv) }
        }
    }

    is FilterPaired -> {
        pred.requireInputs("$path.pred", Scope.Scalar)
        val innerPlan = requirePaired(inner, "$path.inner").compilePaired("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            val m = innerPlan.build(concurrency)
            FilterPairedStat(m) { xv, yv -> pred.eval(xv, yv) }
        }
    }

    is FoldPaired -> {
        expr.requireInputs("$path.expr", Scope.Scalar)
        val innerPlan = requireSeries(inner, "$path.inner").compileSeries("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            val m = innerPlan.build(concurrency)
            FoldPairedStat(m) { xv, yv -> expr.eval(xv, yv) }
        }
    }

    is WeightByValuePaired -> {
        expr.requireInputs("$path.expr", Scope.Scalar)
        val innerPlan = requirePaired(inner, "$path.inner").compilePaired("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).weightBy { xv, yv -> expr.eval(xv, yv) }
        }
    }

    is ThrottlePaired -> {
        val innerPlan = requirePaired(inner, "$path.inner").compilePaired("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).throttle(every)
        }
    }

    is SamplePaired -> {
        val innerPlan = requirePaired(inner, "$path.inner").compilePaired("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).sample(rate, seed)
        }
    }

    is WithFeedbackPaired -> {
        val reads = project.requireInputs("$path.project", Scope.IndexedFeedback)
        val innerPlan = requirePaired(inner, "$path.inner").compilePaired("$path.inner")
        val primaryXPlan = requireSeries(primaryX, "$path.primaryX").compileSeries("$path.primaryX")
        val primaryYPlan = requireSeries(primaryY, "$path.primaryY").compileSeries("$path.primaryY")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            val primaryXStat = primaryXPlan.build(concurrency)
            val primaryYStat = primaryYPlan.build(concurrency)
            reads.requirePrimaries("$path.primaryX", primaryXStat)
            reads.requirePrimaries("$path.primaryY", primaryYStat)
            innerPlan.build(concurrency).withFeedback(
                primaryXStat as SeriesStat<Result>,
                primaryYStat as SeriesStat<Result>,
                project,
            )
        }
    }
}

@Suppress("UNCHECKED_CAST")
internal fun VectorStatSpec<*>.compileVector(path: String): StatPlan<VectorStat<*>> = when (this) {
    is VectorGroupStatSpec -> {
        val plans = stats.map { (name, spec) ->
            name to requireVector(spec, "$path.$name").compileVector("$path.$name")
        }
        StatPlan(path) { concurrency ->
            VectorStatGroup(
                plans.map { (name, plan) ->
                    toSpec(StatKey<Result>(name), plan.build(concurrency))
                },
                concurrency,
            )
        }
    }

    is WithWeightVector -> {
        val innerPlan = requireVector(inner, "$path.inner").compileVector("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).withWeight(weight)
        }
    }

    is WindowedVector -> {
        val innerPlan = requireVector(inner, "$path.inner").compileVector("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan
                .build(concurrency)
                .windowed(durationMillis.milliseconds, slices, concurrency)
        }
    }

    is Vectorized -> {
        val templatePlan =
            requireSeries(template, "$path.template").compileSeries("$path.template")
        StatPlan(path) { concurrency ->
            val tpl = templatePlan.build(concurrency)
            VectorizedStat(dimensions, tpl, skipZeros)
        }
    }

    is TransformVectorElement -> {
        expr.requireInputs("$path.expr", Scope.Vector)
        val innerPlan = requireVector(inner, "$path.inner").compileVector("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            val m = innerPlan.build(concurrency)
            TransformVectorStat.vector(m) { vec ->
                DoubleArray(vec.size) { i -> expr.eval(vec[i], 0.0, vec) }
            }
        }
    }

    is FilterVector -> {
        pred.requireInputs("$path.pred", Scope.Vector)
        val innerPlan = requireVector(inner, "$path.inner").compileVector("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            val m = innerPlan.build(concurrency)
            FilterVectorStat.vector(m) { vec -> pred.eval(0.0, 0.0, vec) }
        }
    }

    is FoldVector -> {
        expr.requireInputs("$path.expr", Scope.Vector)
        val innerPlan = requireSeries(inner, "$path.inner").compileSeries("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            val m = innerPlan.build(concurrency)
            FoldVectorStat.vector(m) { vec -> expr.eval(0.0, 0.0, vec) }
        }
    }

    is FoldVectorPaired -> {
        xExpr.requireInputs("$path.xExpr", Scope.Vector)
        yExpr.requireInputs("$path.yExpr", Scope.Vector)
        val innerPlan = requirePaired(inner, "$path.inner").compilePaired("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            val m = innerPlan.build(concurrency)
            FoldVectorPairedStat.vector(
                m,
                foldX = { vec -> xExpr.eval(0.0, 0.0, vec) },
                foldY = { vec -> yExpr.eval(0.0, 0.0, vec) },
            )
        }
    }

    is TransformVector -> {
        expr.requireInputs("$path.expr", Scope.Vector)
        val innerPlan = requireVector(inner, "$path.inner").compileVector("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            val m = innerPlan.build(concurrency)
            TransformVectorStat.vector(m) { vec -> expr.eval(0.0, 0.0, vec) }
        }
    }

    is WeightByValueVector -> {
        expr.requireInputs("$path.expr", Scope.Vector)
        val innerPlan = requireVector(inner, "$path.inner").compileVector("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            WeightByVectorStat.vector(innerPlan.build(concurrency)) { vec ->
                expr.eval(0.0, 0.0, vec)
            }
        }
    }

    is ThrottleVector -> {
        val innerPlan = requireVector(inner, "$path.inner").compileVector("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).throttle(every)
        }
    }

    is SampleVector -> {
        val innerPlan = requireVector(inner, "$path.inner").compileVector("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).sample(rate, seed)
        }
    }

    is WithFeedbackVector -> {
        val reads = project.requireInputs("$path.project", Scope.IndexedFeedback)
        val innerPlan = requireVector(inner, "$path.inner").compileVector("$path.inner")
        val primaryPlan = requireVector(primary, "$path.primary").compileVector("$path.primary")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            val primaryStat = primaryPlan.build(concurrency)
            val results = primaryStat.read(0L)
            require(results is ResultList<*>) { "$path.primary: expected per-coordinate results" }
            reads.requirePrimaries("$path.primary", results.results)
            innerPlan.build(concurrency).withFeedback(primaryStat as VectorStat<ResultList<Result>>, project)
        }
    }
}

internal fun DiscreteStatSpec<*>.compileDiscrete(path: String): StatPlan<DiscreteStat<*>> = when (this) {
    is DiscreteGroupStatSpec -> {
        val plans = stats.map { (name, spec) ->
            name to requireDiscrete(spec, "$path.$name").compileDiscrete("$path.$name")
        }
        StatPlan(path) { concurrency ->
            DiscreteStatGroup(
                plans.map { (name, plan) ->
                    toSpec(StatKey<Result>(name), plan.build(concurrency))
                },
                concurrency,
            )
        }
    }

    is HyperLogLog ->
        StatPlan(path) { concurrency ->
            HyperLogLogStat(precision, Hashers.resolve(hasher), concurrency)
        }

    is BloomFilter ->
        StatPlan(path) { concurrency ->
            BloomFilterStat(bits, hashes, Hashers.resolve(hasher), concurrency)
        }

    is CountMinSketch ->
        StatPlan(path) { concurrency ->
            CountMinSketchStat(depth, width, seed, Hashers.resolve(hasher), concurrency)
        }

    is SpaceSaving -> StatPlan(path) { concurrency -> SpaceSavingStat(capacity, concurrency) }

    is Sojourn -> StatPlan(path) { concurrency -> SojournStat(states, concurrency) }

    is WithWeightDiscrete -> {
        val innerPlan = requireDiscrete(inner, "$path.inner").compileDiscrete("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).withWeight(weight)
        }
    }

    is WithValueDiscrete -> {
        val innerPlan = requireDiscrete(inner, "$path.inner").compileDiscrete("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).withValue(value)
        }
    }

    is AsDiscrete -> {
        val innerPlan = requireSeries(inner, "$path.inner").compileSeries("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).asDiscrete()
        }
    }

    is WindowedDiscrete -> {
        val innerPlan = requireDiscrete(inner, "$path.inner").compileDiscrete("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan
                .build(concurrency)
                .windowed(durationMillis.milliseconds, slices, concurrency)
        }
    }

    is TransformValueDiscrete -> {
        expr.requireInputs("$path.expr", Scope.Scalar)
        val innerPlan = requireDiscrete(inner, "$path.inner").compileDiscrete("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            val m = innerPlan.build(concurrency)
            if (expr.isDiscreteIdentity()) m else TransformLongStat(m) { expr.eval(it.toDouble()) }
        }
    }

    is FilterValueDiscrete -> {
        pred.requireInputs("$path.pred", Scope.Scalar)
        val innerPlan = requireDiscrete(inner, "$path.inner").compileDiscrete("$path.inner")
        val predicate = pred.discretePredicate()
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            FilterDiscreteStat(innerPlan.build(concurrency), predicate)
        }
    }

    is WeightByValueDiscrete -> {
        expr.requireInputs("$path.expr", Scope.Scalar)
        val innerPlan = requireDiscrete(inner, "$path.inner").compileDiscrete("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).weightBy { v -> expr.eval(v.toDouble()) }
        }
    }

    is ThrottleDiscrete -> {
        val innerPlan = requireDiscrete(inner, "$path.inner").compileDiscrete("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).throttle(every)
        }
    }

    is SampleDiscrete -> {
        val innerPlan = requireDiscrete(inner, "$path.inner").compileDiscrete("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).sample(rate, seed)
        }
    }
}

@Suppress("UNCHECKED_CAST")
internal fun RegressionStatSpec<*>.compileRegression(path: String): StatPlan<RegressionStat<*>> = when (this) {
    is RegressionGroupStatSpec -> {
        val plans = stats.map { (name, spec) ->
            name to requireRegression(spec, "$path.$name").compileRegression("$path.$name")
        }
        StatPlan(path) { concurrency ->
            RegressionStatGroup(
                plans.map { (name, plan) ->
                    toSpec(StatKey<Result>(name), plan.build(concurrency))
                },
                concurrency,
            )
        }
    }

    is BayesianRegression ->
        StatPlan(path) { concurrency ->
            BayesianRegressionStat(featureSize, priorVariance, link, concurrency, priorMean, priorCovariance)
        }

    is StochasticRegression ->
        StatPlan(path) { concurrency ->
            StochasticRegressionStat(
                featureSize,
                optimizer,
                biasOptimizer,
                penalty,
                link,
                concurrency,
            )
        }

    is DiagonalRegression -> {
        learningRate.requireInputs("$path.learningRate", Scope.Scalar)
        StatPlan(path) { concurrency ->
            DiagonalRegressionStat(
                featureSize,
                priorPrecision,
                learningRate,
                penalty,
                link,
                concurrency,
            )
        }
    }

    is SoftmaxRegression ->
        StatPlan(path) { concurrency ->
            SoftmaxRegressionStat(
                featureSize,
                numClasses,
                optimizer,
                biasOptimizer,
                concurrency,
            )
        }

    is GaussianNaiveBayes ->
        StatPlan(path) { concurrency ->
            GaussianNaiveBayesStat(featureSize, numClasses, varianceFloor, concurrency)
        }

    is DecisionTreeRegression -> {
        StatPlan(path) { concurrency ->
            DecisionTreeRegressionStat(
                featureSize = featureSize,
                splitCandidates = splitCandidates,
                config = config,
                concurrency = concurrency,
                randomSeed = randomSeed,
                leafArmFactory = leafArm?.leafFactory("$path.leafArm"),
            )
        }
    }

    is RandomForestRegression -> {
        StatPlan(path) { concurrency ->
            RandomForestRegressionStat(
                featureSize = featureSize,
                splitCandidates = splitCandidates,
                nbrTrees = nbrTrees,
                config = config,
                bagging = bagging,
                concurrency = concurrency,
                randomSeed = randomSeed,
                leafArmFactory = leafArm?.leafFactory("$path.leafArm"),
            )
        }
    }

    is DecisionTreeClassifier -> {
        StatPlan(path) { concurrency ->
            DecisionTreeClassifierStat(
                featureSize = featureSize,
                numClasses = numClasses,
                splitCandidates = splitCandidates,
                config = config,
                concurrency = concurrency,
                randomSeed = randomSeed,
                leafArmFactory = leafArm?.leafFactory("$path.leafArm", numClasses),
            )
        }
    }

    is RandomForestClassifier -> {
        StatPlan(path) { concurrency ->
            RandomForestClassifierStat(
                featureSize = featureSize,
                numClasses = numClasses,
                splitCandidates = splitCandidates,
                nbrTrees = nbrTrees,
                config = config,
                bagging = bagging,
                concurrency = concurrency,
                randomSeed = randomSeed,
                leafArmFactory = leafArm?.leafFactory("$path.leafArm", numClasses),
            )
        }
    }

    is FilterRegression -> {
        pred.requireInputs("$path.pred", Scope.Vector)
        val innerPlan = requireRegression(inner, "$path.inner").compileRegression("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            val m = innerPlan.build(concurrency)
            m.filter { v, y -> pred.eval(0.0, y, v) }
        }
    }

    is TransformYRegression -> {
        expr.requireInputs("$path.expr", Scope.Vector)
        val innerPlan = requireRegression(inner, "$path.inner").compileRegression("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            val m = innerPlan.build(concurrency)
            m.transformY { v, y -> expr.eval(0.0, y, v) }
        }
    }

    is TransformXRegression -> {
        expr.requireInputs("$path.expr", Scope.Vector)
        val innerPlan = requireRegression(inner, "$path.inner").compileRegression("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            val m = innerPlan.build(concurrency)
            m.transformX(featureSize) { v, y -> expr.eval(0.0, y, v) }
        }
    }

    is WithWeightRegression -> {
        val innerPlan = requireRegression(inner, "$path.inner").compileRegression("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).withWeight(weight)
        }
    }

    is WeightByValueRegression -> {
        expr.requireInputs("$path.expr", Scope.Vector)
        val innerPlan = requireRegression(inner, "$path.inner").compileRegression("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            val m = innerPlan.build(concurrency)
            m.weightBy { v, y -> expr.eval(0.0, y, v) }
        }
    }

    is ThrottleRegression -> {
        val innerPlan = requireRegression(inner, "$path.inner").compileRegression("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).throttle(every)
        }
    }

    is SampleRegression -> {
        val innerPlan = requireRegression(inner, "$path.inner").compileRegression("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).sample(rate, seed)
        }
    }

    is FoldRegression -> {
        project.requireInputs("$path.project", Scope.Vector)
        val innerPlan = requireSeries(inner, "$path.inner").compileSeries("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            val m = innerPlan.build(concurrency)
            m.foldRegression(featureSize) { v, y -> project.eval(0.0, y, v) }
        }
    }

    is WithFeedbackRegression -> {
        val reads = project.requireInputs("$path.project", Scope.IndexedFeedback)
        val innerPlan = requireRegression(inner, "$path.inner").compileRegression("$path.inner")
        val primaryPlan = requireVector(primary, "$path.primary").compileVector("$path.primary")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            val primaryStat = primaryPlan.build(concurrency)
            val innerStat = innerPlan.build(concurrency)
            val results = primaryStat.read(0L)
            require(results is ResultList<*> && results.results.size == innerStat.featureSize) {
                "$path.primary: expected ${innerStat.featureSize} per-coordinate results"
            }
            reads.requirePrimaries("$path.primary", results.results)
            innerStat.withFeedback(primaryStat as VectorStat<ResultList<Result>>, project)
        }
    }

    is StandardScalerRegression -> {
        val innerPlan = requireRegression(inner, "$path.inner").compileRegression("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).standardScaleFeatures(concurrency)
        }
    }

    is MinMaxScalerRegression -> {
        val innerPlan = requireRegression(inner, "$path.inner").compileRegression("$path.inner")
        StatPlan(path, hasCenterScale = innerPlan.hasCenterScale) { concurrency ->
            innerPlan.build(concurrency).minMaxScaleFeatures(targetLow, targetHigh, concurrency)
        }
    }
}

@Suppress("UNCHECKED_CAST")
private inline fun <reified R : Result> SeriesStatSpec<R>.leafFactory(
    path: String,
    numClasses: Int? = null,
): (Concurrency) -> SeriesStat<R> {
    val plan = compileSeries(path)
    return { concurrency ->
        val stat = plan.build(concurrency)
        val result = stat.read(0L)
        require(result is R) { "$path: incompatible leaf result" }
        require(numClasses == null || (result is ClassCountsResult && result.numClasses == numClasses)) {
            "$path: expected $numClasses classes"
        }
        stat as SeriesStat<R>
    }
}

private inline fun <reified S : StatSpec> requireInner(inner: StatSpec, path: String): S {
    require(inner is S) { "$path: expected ${S::class.simpleName}, got ${inner::class.simpleName}" }
    return inner
}

private fun requireSeries(inner: StatSpec, path: String): SeriesStatSpec<*> = requireInner(inner, path)

private fun requirePaired(inner: StatSpec, path: String): PairedStatSpec<*> = requireInner(inner, path)

private fun requireVector(inner: StatSpec, path: String): VectorStatSpec<*> = requireInner(inner, path)

private fun requireDiscrete(inner: StatSpec, path: String): DiscreteStatSpec<*> = requireInner(inner, path)

private fun requireRegression(inner: StatSpec, path: String): RegressionStatSpec<*> = requireInner(inner, path)

private fun ScalarExpr.requireInputs(path: String, scope: Scope): ExprInputs {
    requireBound(path)
    return inputs().also { it.requireScope(path, scope) }
}

private fun BoolExpr.requireInputs(path: String, scope: Scope): ExprInputs {
    requireBound(path)
    return inputs().also { it.requireScope(path, scope) }
}

private fun VectorExpr.requireInputs(path: String, scope: Scope): ExprInputs {
    requireBound(path)
    return inputs().also { it.requireScope(path, scope) }
}

// An empty snapshot carries the same result type as every later one, so checking it here
// rejects a projection the primary cannot serve before any update reaches either stat.
private fun ExprInputs.requirePrimaries(path: String, primary: Stat<*>) {
    if (readsFeedback) requirePrimary(path, primary.read(0L))
}

private fun ExprInputs.requirePrimaries(path: String, results: List<Result>) {
    if (readsFeedback) results.forEach { requirePrimary(path, it) }
}
