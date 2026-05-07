package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.stat.cardinality.HyperLogLog
import com.eignex.kumulant.stat.cardinality.HyperLogLogResult
import com.eignex.kumulant.stat.cardinality.LinearCounting
import com.eignex.kumulant.stat.cardinality.LinearCountingResult
import com.eignex.kumulant.stat.quantile.DDSketch
import com.eignex.kumulant.stat.quantile.FrugalQuantile
import com.eignex.kumulant.stat.quantile.HdrHistogram
import com.eignex.kumulant.stat.quantile.LinearHistogram
import com.eignex.kumulant.stat.quantile.QuantileResult
import com.eignex.kumulant.stat.quantile.ReservoirHistogram
import com.eignex.kumulant.stat.quantile.ReservoirResult
import com.eignex.kumulant.stat.quantile.SketchResult
import com.eignex.kumulant.stat.quantile.SparseHistogramResult
import com.eignex.kumulant.stat.quantile.TDigest
import com.eignex.kumulant.stat.quantile.TDigestResult
import com.eignex.kumulant.stat.rate.CounterRate
import com.eignex.kumulant.stat.rate.Rate
import com.eignex.kumulant.stat.rate.RateResult
import com.eignex.kumulant.stat.regression.Covariance
import com.eignex.kumulant.stat.regression.CovarianceResult
import com.eignex.kumulant.stat.regression.Lasso
import com.eignex.kumulant.stat.regression.LassoResult
import com.eignex.kumulant.stat.regression.OLS
import com.eignex.kumulant.stat.regression.OLSResult
import com.eignex.kumulant.stat.regression.Ridge
import com.eignex.kumulant.stat.regression.RidgeResult
import com.eignex.kumulant.stat.score.Auc
import com.eignex.kumulant.stat.score.AucResult
import com.eignex.kumulant.stat.score.BrierScore
import com.eignex.kumulant.stat.score.LogLoss
import com.eignex.kumulant.stat.score.MaeLoss
import com.eignex.kumulant.stat.score.MseLoss
import com.eignex.kumulant.stat.score.PinballLoss
import com.eignex.kumulant.stat.score.Reliability
import com.eignex.kumulant.stat.score.ReliabilityResult
import com.eignex.kumulant.stat.score.pitHistogram
import com.eignex.kumulant.stat.sketch.BloomFilter
import com.eignex.kumulant.stat.sketch.BloomFilterResult
import com.eignex.kumulant.stat.sketch.CountMinSketch
import com.eignex.kumulant.stat.sketch.CountMinSketchResult
import com.eignex.kumulant.stat.sketch.HeavyHittersResult
import com.eignex.kumulant.stat.sketch.MinHash
import com.eignex.kumulant.stat.sketch.MinHashResult
import com.eignex.kumulant.stat.sketch.SpaceSaving
import com.eignex.kumulant.stat.summary.BernoulliSum
import com.eignex.kumulant.stat.summary.BernoulliSumResult
import com.eignex.kumulant.stat.summary.Count
import com.eignex.kumulant.stat.summary.Max
import com.eignex.kumulant.stat.summary.MaxResult
import com.eignex.kumulant.stat.summary.Mean
import com.eignex.kumulant.stat.summary.Min
import com.eignex.kumulant.stat.summary.MinResult
import com.eignex.kumulant.stat.summary.Moments
import com.eignex.kumulant.stat.summary.MomentsResult
import com.eignex.kumulant.stat.summary.PairedSum
import com.eignex.kumulant.stat.summary.PairedSumResult
import com.eignex.kumulant.stat.summary.Range
import com.eignex.kumulant.stat.summary.RangeResult
import com.eignex.kumulant.stat.summary.Sum
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stat.summary.TotalWeights
import com.eignex.kumulant.stat.summary.Variance
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import com.eignex.kumulant.stat.summary.varianceVector
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * [StatConfig] variants for kumulant's stats. Co-located here (rather than next to each
 * stat) because Kotlin requires direct subclasses of a sealed interface to live in the
 * same package as the interface.
 *
 * Each variant uses `@SerialName` matching its Kotlin class name, so the wire `$type`
 * value mirrors what a Kotlin reader would type. Defaults match the underlying stat's
 * primary constructor so authored payloads stay terse under `encodeDefaults = false`.
 */

// ========== Series ==========

@Serializable
@SerialName("MeanConfig")
data object MeanConfig : SeriesStatConfig<WeightedMeanResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<WeightedMeanResult> = Mean(concurrency)
}

@Serializable
@SerialName("SumConfig")
data object SumConfig : SeriesStatConfig<SumResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<SumResult> = Sum(concurrency)
}

@Serializable
@SerialName("MinConfig")
data object MinConfig : SeriesStatConfig<MinResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<MinResult> = Min(concurrency)
}

@Serializable
@SerialName("MaxConfig")
data object MaxConfig : SeriesStatConfig<MaxResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<MaxResult> = Max(concurrency)
}

@Serializable
@SerialName("RangeConfig")
data object RangeConfig : SeriesStatConfig<RangeResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<RangeResult> = Range(concurrency)
}

@Serializable
@SerialName("VarianceConfig")
data object VarianceConfig : SeriesStatConfig<WeightedVarianceResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<WeightedVarianceResult> = Variance(concurrency)
}

@Serializable
@SerialName("MomentsConfig")
data object MomentsConfig : SeriesStatConfig<MomentsResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<MomentsResult> = Moments(concurrency)
}

@Serializable
@SerialName("BernoulliSumConfig")
data object BernoulliSumConfig : SeriesStatConfig<BernoulliSumResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<BernoulliSumResult> = BernoulliSum(concurrency)
}

@Serializable
@SerialName("TotalWeightsConfig")
data object TotalWeightsConfig : SeriesStatConfig<SumResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<SumResult> = TotalWeights(concurrency)
}

@Serializable
@SerialName("CountConfig")
data object CountConfig : SeriesStatConfig<SumResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<SumResult> = Count(concurrency)
}

@Serializable
@SerialName("RateConfig")
data object RateConfig : SeriesStatConfig<RateResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<RateResult> = Rate(concurrency)
}

@Serializable
@SerialName("CounterRateConfig")
data class CounterRateConfig(
    val treatDecreaseAsReset: Boolean = true,
) : SeriesStatConfig<RateResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<RateResult> =
        CounterRate(concurrency, treatDecreaseAsReset)
}

/**
 * Configuration for [DDSketch]. `probabilities` is a [List] on the wire because YAML
 * renders lists more cleanly than primitive arrays; converted to a `DoubleArray` at
 * [materialize] time.
 */
@Serializable
@SerialName("DDSketchConfig")
data class DDSketchConfig(
    val relativeError: Double = 0.01,
    val probabilities: List<Double> = listOf(0.5, 0.75, 0.9, 0.95, 0.99, 0.999),
) : SeriesStatConfig<SketchResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<SketchResult> =
        DDSketch(relativeError, probabilities.toDoubleArray(), concurrency)
}

@Serializable
@SerialName("FrugalQuantileConfig")
data class FrugalQuantileConfig(
    val q: Double,
    val stepSize: Double = 0.01,
    val initialEstimate: Double = 0.0,
) : SeriesStatConfig<QuantileResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<QuantileResult> =
        FrugalQuantile(q, stepSize, initialEstimate, concurrency)
}

@Serializable
@SerialName("HdrHistogramConfig")
data class HdrHistogramConfig(
    val lowestDiscernibleValue: Double = 0.001,
    val initialHighestTrackableValue: Double = 100.0,
    val significantDigits: Int = 3,
) : SeriesStatConfig<SparseHistogramResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<SparseHistogramResult> =
        HdrHistogram(lowestDiscernibleValue, initialHighestTrackableValue, significantDigits, concurrency)
}

@Serializable
@SerialName("LinearHistogramConfig")
data class LinearHistogramConfig(
    val lowerBound: Double,
    val upperBound: Double,
    val binCount: Int,
) : SeriesStatConfig<SparseHistogramResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<SparseHistogramResult> =
        LinearHistogram(lowerBound, upperBound, binCount, concurrency)
}

/**
 * Configuration for [ReservoirHistogram]. Seed has no default — the live constructor's
 * `Random.Default.nextLong()` is non-deterministic, which would silently produce
 * different goldens on each instantiation if mirrored here.
 */
@Serializable
@SerialName("ReservoirHistogramConfig")
data class ReservoirHistogramConfig(
    val capacity: Int = 1024,
    val seed: Long,
) : SeriesStatConfig<ReservoirResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<ReservoirResult> =
        ReservoirHistogram(capacity, seed, concurrency)
}

@Serializable
@SerialName("TDigestConfig")
data class TDigestConfig(
    val compression: Double = 100.0,
    val probabilities: List<Double> = listOf(0.5, 0.75, 0.9, 0.95, 0.99, 0.999),
) : SeriesStatConfig<TDigestResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<TDigestResult> =
        TDigest(compression, probabilities.toDoubleArray(), concurrency)
}

@Serializable
@SerialName("PitHistogramConfig")
data class PitHistogramConfig(val numBins: Int) : SeriesStatConfig<SparseHistogramResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<SparseHistogramResult> =
        pitHistogram(numBins, concurrency)
}

// ========== Paired ==========

@Serializable
@SerialName("PairedSumConfig")
data object PairedSumConfig : PairedStatConfig<PairedSumResult> {
    override fun materialize(concurrency: Concurrency): PairedStat<PairedSumResult> = PairedSum(concurrency)
}

@Serializable
@SerialName("OLSConfig")
data object OLSConfig : PairedStatConfig<OLSResult> {
    override fun materialize(concurrency: Concurrency): PairedStat<OLSResult> = OLS(concurrency)
}

@Serializable
@SerialName("CovarianceConfig")
data object CovarianceConfig : PairedStatConfig<CovarianceResult> {
    override fun materialize(concurrency: Concurrency): PairedStat<CovarianceResult> = Covariance(concurrency)
}

@Serializable
@SerialName("LassoConfig")
data class LassoConfig(val lambda: Double) : PairedStatConfig<LassoResult> {
    override fun materialize(concurrency: Concurrency): PairedStat<LassoResult> = Lasso(lambda, concurrency)
}

@Serializable
@SerialName("RidgeConfig")
data class RidgeConfig(val lambda: Double) : PairedStatConfig<RidgeResult> {
    override fun materialize(concurrency: Concurrency): PairedStat<RidgeResult> = Ridge(lambda, concurrency)
}

@Serializable
@SerialName("BrierScoreConfig")
data object BrierScoreConfig : PairedStatConfig<WeightedMeanResult> {
    override fun materialize(concurrency: Concurrency): PairedStat<WeightedMeanResult> = BrierScore(concurrency)
}

@Serializable
@SerialName("MseLossConfig")
data object MseLossConfig : PairedStatConfig<WeightedMeanResult> {
    override fun materialize(concurrency: Concurrency): PairedStat<WeightedMeanResult> = MseLoss(concurrency)
}

@Serializable
@SerialName("MaeLossConfig")
data object MaeLossConfig : PairedStatConfig<WeightedMeanResult> {
    override fun materialize(concurrency: Concurrency): PairedStat<WeightedMeanResult> = MaeLoss(concurrency)
}

@Serializable
@SerialName("LogLossConfig")
data object LogLossConfig : PairedStatConfig<WeightedMeanResult> {
    override fun materialize(concurrency: Concurrency): PairedStat<WeightedMeanResult> = LogLoss(concurrency)
}

@Serializable
@SerialName("PinballLossConfig")
data class PinballLossConfig(val tau: Double) : PairedStatConfig<WeightedMeanResult> {
    override fun materialize(concurrency: Concurrency): PairedStat<WeightedMeanResult> =
        PinballLoss(tau, concurrency)
}

@Serializable
@SerialName("AucConfig")
data class AucConfig(
    val numBins: Int = 256,
    val lowerBound: Double = 0.0,
    val upperBound: Double = 1.0,
) : PairedStatConfig<AucResult> {
    override fun materialize(concurrency: Concurrency): PairedStat<AucResult> =
        Auc(numBins, lowerBound, upperBound, concurrency)
}

@Serializable
@SerialName("ReliabilityConfig")
data class ReliabilityConfig(val numBins: Int) : PairedStatConfig<ReliabilityResult> {
    override fun materialize(concurrency: Concurrency): PairedStat<ReliabilityResult> =
        Reliability(numBins, concurrency)
}

// ========== Vector ==========

@Serializable
@SerialName("VarianceVectorConfig")
data class VarianceVectorConfig(val dimensions: Int) : VectorStatConfig<ResultList<WeightedVarianceResult>> {
    override fun materialize(concurrency: Concurrency): VectorStat<ResultList<WeightedVarianceResult>> =
        varianceVector(dimensions, concurrency)
}

// ========== Discrete ==========

@Serializable
@SerialName("HyperLogLogConfig")
data class HyperLogLogConfig(val precision: Int = 14) : DiscreteStatConfig<HyperLogLogResult> {
    override fun materialize(concurrency: Concurrency): DiscreteStat<HyperLogLogResult> =
        HyperLogLog(precision, concurrency)
}

@Serializable
@SerialName("LinearCountingConfig")
data class LinearCountingConfig(val bits: Int = 4096) : DiscreteStatConfig<LinearCountingResult> {
    override fun materialize(concurrency: Concurrency): DiscreteStat<LinearCountingResult> =
        LinearCounting(bits, concurrency)
}

@Serializable
@SerialName("BloomFilterConfig")
data class BloomFilterConfig(
    val bits: Int = 1 shl 16,
    val hashes: Int = 7,
) : DiscreteStatConfig<BloomFilterResult> {
    override fun materialize(concurrency: Concurrency): DiscreteStat<BloomFilterResult> =
        BloomFilter(bits, hashes, concurrency)
}

@Serializable
@SerialName("CountMinSketchConfig")
data class CountMinSketchConfig(
    val depth: Int = 5,
    val width: Int = 1024,
    val seed: Long = -7046029254386353133L,
) : DiscreteStatConfig<CountMinSketchResult> {
    override fun materialize(concurrency: Concurrency): DiscreteStat<CountMinSketchResult> =
        CountMinSketch(depth, width, seed, concurrency)
}

@Serializable
@SerialName("MinHashConfig")
data class MinHashConfig(
    val numHashes: Int = 128,
    val seed: Long = -3724518991637283867L,
) : DiscreteStatConfig<MinHashResult> {
    override fun materialize(concurrency: Concurrency): DiscreteStat<MinHashResult> =
        MinHash(numHashes, seed, concurrency)
}

@Serializable
@SerialName("SpaceSavingConfig")
data class SpaceSavingConfig(val capacity: Int) : DiscreteStatConfig<HeavyHittersResult> {
    override fun materialize(concurrency: Concurrency): DiscreteStat<HeavyHittersResult> =
        SpaceSaving(capacity, concurrency)
}
