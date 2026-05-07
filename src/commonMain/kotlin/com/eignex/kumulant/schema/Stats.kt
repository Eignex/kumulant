package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.stat.cardinality.HyperLogLogResult
import com.eignex.kumulant.stat.cardinality.HyperLogLogStat
import com.eignex.kumulant.stat.cardinality.LinearCountingResult
import com.eignex.kumulant.stat.cardinality.LinearCountingStat
import com.eignex.kumulant.stat.quantile.DDSketchStat
import com.eignex.kumulant.stat.quantile.FrugalQuantileStat
import com.eignex.kumulant.stat.quantile.HdrHistogramStat
import com.eignex.kumulant.stat.quantile.LinearHistogramStat
import com.eignex.kumulant.stat.quantile.QuantileResult
import com.eignex.kumulant.stat.quantile.ReservoirHistogramStat
import com.eignex.kumulant.stat.quantile.ReservoirResult
import com.eignex.kumulant.stat.quantile.SketchResult
import com.eignex.kumulant.stat.quantile.SparseHistogramResult
import com.eignex.kumulant.stat.quantile.TDigestResult
import com.eignex.kumulant.stat.quantile.TDigestStat
import com.eignex.kumulant.stat.rate.CounterRateStat
import com.eignex.kumulant.stat.rate.RateResult
import com.eignex.kumulant.stat.rate.RateStat
import com.eignex.kumulant.stat.regression.CovarianceResult
import com.eignex.kumulant.stat.regression.CovarianceStat
import com.eignex.kumulant.stat.regression.LassoResult
import com.eignex.kumulant.stat.regression.LassoStat
import com.eignex.kumulant.stat.regression.OLSResult
import com.eignex.kumulant.stat.regression.OLSStat
import com.eignex.kumulant.stat.regression.RidgeResult
import com.eignex.kumulant.stat.regression.RidgeStat
import com.eignex.kumulant.stat.score.AucResult
import com.eignex.kumulant.stat.score.AucStat
import com.eignex.kumulant.stat.score.BrierScoreStat
import com.eignex.kumulant.stat.score.LogLossStat
import com.eignex.kumulant.stat.score.MaeLossStat
import com.eignex.kumulant.stat.score.MseLossStat
import com.eignex.kumulant.stat.score.PinballLossStat
import com.eignex.kumulant.stat.score.ReliabilityResult
import com.eignex.kumulant.stat.score.ReliabilityStat
import com.eignex.kumulant.stat.score.pitHistogram
import com.eignex.kumulant.stat.sketch.BloomFilterResult
import com.eignex.kumulant.stat.sketch.BloomFilterStat
import com.eignex.kumulant.stat.sketch.CountMinSketchResult
import com.eignex.kumulant.stat.sketch.CountMinSketchStat
import com.eignex.kumulant.stat.sketch.HeavyHittersResult
import com.eignex.kumulant.stat.sketch.MinHashResult
import com.eignex.kumulant.stat.sketch.MinHashStat
import com.eignex.kumulant.stat.sketch.SpaceSavingStat
import com.eignex.kumulant.stat.summary.BernoulliSumResult
import com.eignex.kumulant.stat.summary.BernoulliSumStat
import com.eignex.kumulant.stat.summary.CountStat
import com.eignex.kumulant.stat.summary.MaxResult
import com.eignex.kumulant.stat.summary.MaxStat
import com.eignex.kumulant.stat.summary.MeanStat
import com.eignex.kumulant.stat.summary.MinResult
import com.eignex.kumulant.stat.summary.MinStat
import com.eignex.kumulant.stat.summary.MomentsResult
import com.eignex.kumulant.stat.summary.MomentsStat
import com.eignex.kumulant.stat.summary.PairedSumResult
import com.eignex.kumulant.stat.summary.PairedSumStat
import com.eignex.kumulant.stat.summary.RangeResult
import com.eignex.kumulant.stat.summary.RangeStat
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stat.summary.SumStat
import com.eignex.kumulant.stat.summary.TotalWeightsStat
import com.eignex.kumulant.stat.summary.VarianceStat
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import com.eignex.kumulant.stat.summary.varianceVector
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * [StatSpec] variants for kumulant's stats. Co-located here (rather than next to each
 * stat) because Kotlin requires direct subclasses of a sealed interface to live in the
 * same package as the interface.
 *
 * Each variant uses `@SerialName` matching its Kotlin class name, so the wire `$type`
 * value mirrors what a Kotlin reader would type. Defaults match the underlying stat's
 * primary constructor so authored payloads stay terse under `encodeDefaults = false`.
 */

// ========== Series ==========

@Serializable
@SerialName("Mean")
data object Mean : SeriesStatSpec<WeightedMeanResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<WeightedMeanResult> = MeanStat(concurrency)
}

@Serializable
@SerialName("Sum")
data object Sum : SeriesStatSpec<SumResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<SumResult> = SumStat(concurrency)
}

@Serializable
@SerialName("Min")
data object Min : SeriesStatSpec<MinResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<MinResult> = MinStat(concurrency)
}

@Serializable
@SerialName("Max")
data object Max : SeriesStatSpec<MaxResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<MaxResult> = MaxStat(concurrency)
}

@Serializable
@SerialName("Range")
data object Range : SeriesStatSpec<RangeResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<RangeResult> = RangeStat(concurrency)
}

@Serializable
@SerialName("Variance")
data object Variance : SeriesStatSpec<WeightedVarianceResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<WeightedVarianceResult> = VarianceStat(concurrency)
}

@Serializable
@SerialName("Moments")
data object Moments : SeriesStatSpec<MomentsResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<MomentsResult> = MomentsStat(concurrency)
}

@Serializable
@SerialName("BernoulliSum")
data object BernoulliSum : SeriesStatSpec<BernoulliSumResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<BernoulliSumResult> = BernoulliSumStat(concurrency)
}

@Serializable
@SerialName("TotalWeights")
data object TotalWeights : SeriesStatSpec<SumResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<SumResult> = TotalWeightsStat(concurrency)
}

@Serializable
@SerialName("Count")
data object Count : SeriesStatSpec<SumResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<SumResult> = CountStat(concurrency)
}

@Serializable
@SerialName("Rate")
data object Rate : SeriesStatSpec<RateResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<RateResult> = RateStat(concurrency)
}

@Serializable
@SerialName("CounterRate")
data class CounterRate(
    val treatDecreaseAsReset: Boolean = true,
) : SeriesStatSpec<RateResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<RateResult> =
        CounterRateStat(concurrency, treatDecreaseAsReset)
}

/**
 * Configuration for [DDSketchStat]. `probabilities` is a [List] on the wire because YAML
 * renders lists more cleanly than primitive arrays; converted to a `DoubleArray` at
 * [materialize] time.
 */
@Serializable
@SerialName("DDSketch")
data class DDSketch(
    val relativeError: Double = 0.01,
    val probabilities: List<Double> = listOf(0.5, 0.75, 0.9, 0.95, 0.99, 0.999),
) : SeriesStatSpec<SketchResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<SketchResult> =
        DDSketchStat(relativeError, probabilities.toDoubleArray(), concurrency)
}

@Serializable
@SerialName("FrugalQuantile")
data class FrugalQuantile(
    val q: Double,
    val stepSize: Double = 0.01,
    val initialEstimate: Double = 0.0,
) : SeriesStatSpec<QuantileResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<QuantileResult> =
        FrugalQuantileStat(q, stepSize, initialEstimate, concurrency)
}

@Serializable
@SerialName("HdrHistogram")
data class HdrHistogram(
    val lowestDiscernibleValue: Double = 0.001,
    val initialHighestTrackableValue: Double = 100.0,
    val significantDigits: Int = 3,
) : SeriesStatSpec<SparseHistogramResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<SparseHistogramResult> =
        HdrHistogramStat(lowestDiscernibleValue, initialHighestTrackableValue, significantDigits, concurrency)
}

@Serializable
@SerialName("LinearHistogram")
data class LinearHistogram(
    val lowerBound: Double,
    val upperBound: Double,
    val binCount: Int,
) : SeriesStatSpec<SparseHistogramResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<SparseHistogramResult> =
        LinearHistogramStat(lowerBound, upperBound, binCount, concurrency)
}

/**
 * Configuration for [ReservoirHistogramStat]. Seed has no default — the live constructor's
 * `Random.Default.nextLong()` is non-deterministic, which would silently produce
 * different goldens on each instantiation if mirrored here.
 */
@Serializable
@SerialName("ReservoirHistogram")
data class ReservoirHistogram(
    val capacity: Int = 1024,
    val seed: Long,
) : SeriesStatSpec<ReservoirResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<ReservoirResult> =
        ReservoirHistogramStat(capacity, seed, concurrency)
}

@Serializable
@SerialName("TDigest")
data class TDigest(
    val compression: Double = 100.0,
    val probabilities: List<Double> = listOf(0.5, 0.75, 0.9, 0.95, 0.99, 0.999),
) : SeriesStatSpec<TDigestResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<TDigestResult> =
        TDigestStat(compression, probabilities.toDoubleArray(), concurrency)
}

@Serializable
@SerialName("PitHistogram")
data class PitHistogram(val numBins: Int) : SeriesStatSpec<SparseHistogramResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<SparseHistogramResult> =
        pitHistogram(numBins, concurrency)
}

// ========== Paired ==========

@Serializable
@SerialName("PairedSum")
data object PairedSum : PairedStatSpec<PairedSumResult> {
    override fun materialize(concurrency: Concurrency): PairedStat<PairedSumResult> = PairedSumStat(concurrency)
}

@Serializable
@SerialName("OLS")
data object OLS : PairedStatSpec<OLSResult> {
    override fun materialize(concurrency: Concurrency): PairedStat<OLSResult> = OLSStat(concurrency)
}

@Serializable
@SerialName("Covariance")
data object Covariance : PairedStatSpec<CovarianceResult> {
    override fun materialize(concurrency: Concurrency): PairedStat<CovarianceResult> = CovarianceStat(concurrency)
}

@Serializable
@SerialName("Lasso")
data class Lasso(val lambda: Double) : PairedStatSpec<LassoResult> {
    override fun materialize(concurrency: Concurrency): PairedStat<LassoResult> = LassoStat(lambda, concurrency)
}

@Serializable
@SerialName("Ridge")
data class Ridge(val lambda: Double) : PairedStatSpec<RidgeResult> {
    override fun materialize(concurrency: Concurrency): PairedStat<RidgeResult> = RidgeStat(lambda, concurrency)
}

@Serializable
@SerialName("BrierScore")
data object BrierScore : PairedStatSpec<WeightedMeanResult> {
    override fun materialize(concurrency: Concurrency): PairedStat<WeightedMeanResult> = BrierScoreStat(concurrency)
}

@Serializable
@SerialName("MseLoss")
data object MseLoss : PairedStatSpec<WeightedMeanResult> {
    override fun materialize(concurrency: Concurrency): PairedStat<WeightedMeanResult> = MseLossStat(concurrency)
}

@Serializable
@SerialName("MaeLoss")
data object MaeLoss : PairedStatSpec<WeightedMeanResult> {
    override fun materialize(concurrency: Concurrency): PairedStat<WeightedMeanResult> = MaeLossStat(concurrency)
}

@Serializable
@SerialName("LogLoss")
data object LogLoss : PairedStatSpec<WeightedMeanResult> {
    override fun materialize(concurrency: Concurrency): PairedStat<WeightedMeanResult> = LogLossStat(concurrency)
}

@Serializable
@SerialName("PinballLoss")
data class PinballLoss(val tau: Double) : PairedStatSpec<WeightedMeanResult> {
    override fun materialize(concurrency: Concurrency): PairedStat<WeightedMeanResult> =
        PinballLossStat(tau, concurrency)
}

@Serializable
@SerialName("Auc")
data class Auc(
    val numBins: Int = 256,
    val lowerBound: Double = 0.0,
    val upperBound: Double = 1.0,
) : PairedStatSpec<AucResult> {
    override fun materialize(concurrency: Concurrency): PairedStat<AucResult> =
        AucStat(numBins, lowerBound, upperBound, concurrency)
}

@Serializable
@SerialName("Reliability")
data class Reliability(val numBins: Int) : PairedStatSpec<ReliabilityResult> {
    override fun materialize(concurrency: Concurrency): PairedStat<ReliabilityResult> =
        ReliabilityStat(numBins, concurrency)
}

// ========== Vector ==========

@Serializable
@SerialName("VarianceVector")
data class VarianceVector(val dimensions: Int) : VectorStatSpec<ResultList<WeightedVarianceResult>> {
    override fun materialize(concurrency: Concurrency): VectorStat<ResultList<WeightedVarianceResult>> =
        varianceVector(dimensions, concurrency)
}

// ========== Discrete ==========

@Serializable
@SerialName("HyperLogLog")
data class HyperLogLog(val precision: Int = 14) : DiscreteStatSpec<HyperLogLogResult> {
    override fun materialize(concurrency: Concurrency): DiscreteStat<HyperLogLogResult> =
        HyperLogLogStat(precision, concurrency)
}

@Serializable
@SerialName("LinearCounting")
data class LinearCounting(val bits: Int = 4096) : DiscreteStatSpec<LinearCountingResult> {
    override fun materialize(concurrency: Concurrency): DiscreteStat<LinearCountingResult> =
        LinearCountingStat(bits, concurrency)
}

@Serializable
@SerialName("BloomFilter")
data class BloomFilter(
    val bits: Int = 1 shl 16,
    val hashes: Int = 7,
) : DiscreteStatSpec<BloomFilterResult> {
    override fun materialize(concurrency: Concurrency): DiscreteStat<BloomFilterResult> =
        BloomFilterStat(bits, hashes, concurrency)
}

@Serializable
@SerialName("CountMinSketch")
data class CountMinSketch(
    val depth: Int = 5,
    val width: Int = 1024,
    val seed: Long = -7046029254386353133L,
) : DiscreteStatSpec<CountMinSketchResult> {
    override fun materialize(concurrency: Concurrency): DiscreteStat<CountMinSketchResult> =
        CountMinSketchStat(depth, width, seed, concurrency)
}

@Serializable
@SerialName("MinHash")
data class MinHash(
    val numHashes: Int = 128,
    val seed: Long = -3724518991637283867L,
) : DiscreteStatSpec<MinHashResult> {
    override fun materialize(concurrency: Concurrency): DiscreteStat<MinHashResult> =
        MinHashStat(numHashes, seed, concurrency)
}

@Serializable
@SerialName("SpaceSaving")
data class SpaceSaving(val capacity: Int) : DiscreteStatSpec<HeavyHittersResult> {
    override fun materialize(concurrency: Concurrency): DiscreteStat<HeavyHittersResult> =
        SpaceSavingStat(capacity, concurrency)
}
