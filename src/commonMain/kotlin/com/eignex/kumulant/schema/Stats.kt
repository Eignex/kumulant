package com.eignex.kumulant.schema

import com.eignex.kumulant.stat.cardinality.HyperLogLogResult
import com.eignex.kumulant.stat.cardinality.LinearCountingResult
import com.eignex.kumulant.stat.quantile.QuantileResult
import com.eignex.kumulant.stat.quantile.ReservoirResult
import com.eignex.kumulant.stat.quantile.SketchResult
import com.eignex.kumulant.stat.quantile.SparseHistogramResult
import com.eignex.kumulant.stat.quantile.TDigestResult
import com.eignex.kumulant.stat.rate.RateResult
import com.eignex.kumulant.stat.regression.CovarianceResult
import com.eignex.kumulant.stat.regression.LassoResult
import com.eignex.kumulant.stat.regression.OLSResult
import com.eignex.kumulant.stat.regression.RidgeResult
import com.eignex.kumulant.stat.score.AucResult
import com.eignex.kumulant.stat.score.ReliabilityResult
import com.eignex.kumulant.stat.sketch.BloomFilterResult
import com.eignex.kumulant.stat.sketch.CountMinSketchResult
import com.eignex.kumulant.stat.sketch.HeavyHittersResult
import com.eignex.kumulant.stat.sketch.MinHashResult
import com.eignex.kumulant.stat.summary.BernoulliSumResult
import com.eignex.kumulant.stat.summary.MaxResult
import com.eignex.kumulant.stat.summary.MinResult
import com.eignex.kumulant.stat.summary.MomentsResult
import com.eignex.kumulant.stat.summary.PairedSumResult
import com.eignex.kumulant.stat.summary.RangeResult
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
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
 *
 * Construction lives in `StatFactory.kt`; specs here are pure data only.
 */

// ========== Series ==========

@Serializable
@SerialName("Mean")
data object Mean : SeriesStatSpec<WeightedMeanResult>

@Serializable
@SerialName("Sum")
data object Sum : SeriesStatSpec<SumResult>

@Serializable
@SerialName("Min")
data object Min : SeriesStatSpec<MinResult>

@Serializable
@SerialName("Max")
data object Max : SeriesStatSpec<MaxResult>

@Serializable
@SerialName("Range")
data object Range : SeriesStatSpec<RangeResult>

@Serializable
@SerialName("Variance")
data object Variance : SeriesStatSpec<WeightedVarianceResult>

@Serializable
@SerialName("Moments")
data object Moments : SeriesStatSpec<MomentsResult>

@Serializable
@SerialName("BernoulliSum")
data object BernoulliSum : SeriesStatSpec<BernoulliSumResult>

@Serializable
@SerialName("TotalWeights")
data object TotalWeights : SeriesStatSpec<SumResult>

@Serializable
@SerialName("Count")
data object Count : SeriesStatSpec<SumResult>

@Serializable
@SerialName("Rate")
data object Rate : SeriesStatSpec<RateResult>

@Serializable
@SerialName("CounterRate")
data class CounterRate(
    val treatDecreaseAsReset: Boolean = true,
) : SeriesStatSpec<RateResult>

/**
 * Spec for `DDSketchStat`. `probabilities` is a [List] on the wire because most
 * formats serialize lists more cleanly than primitive arrays; converted to a
 * `DoubleArray` at materialize time.
 */
@Serializable
@SerialName("DDSketch")
data class DDSketch(
    val relativeError: Double = 0.01,
    val probabilities: List<Double> = listOf(0.5, 0.75, 0.9, 0.95, 0.99, 0.999),
) : SeriesStatSpec<SketchResult>

@Serializable
@SerialName("FrugalQuantile")
data class FrugalQuantile(
    val q: Double,
    val stepSize: Double = 0.01,
    val initialEstimate: Double = 0.0,
) : SeriesStatSpec<QuantileResult>

@Serializable
@SerialName("HdrHistogram")
data class HdrHistogram(
    val lowestDiscernibleValue: Double = 0.001,
    val initialHighestTrackableValue: Double = 100.0,
    val significantDigits: Int = 3,
) : SeriesStatSpec<SparseHistogramResult>

@Serializable
@SerialName("LinearHistogram")
data class LinearHistogram(
    val lowerBound: Double,
    val upperBound: Double,
    val binCount: Int,
) : SeriesStatSpec<SparseHistogramResult>

/**
 * Configuration for `ReservoirHistogramStat`. Seed has no default — the live constructor's
 * `Random.Default.nextLong()` is non-deterministic, which would silently produce
 * different goldens on each instantiation if mirrored here.
 */
@Serializable
@SerialName("ReservoirHistogram")
data class ReservoirHistogram(
    val capacity: Int = 1024,
    val seed: Long,
) : SeriesStatSpec<ReservoirResult>

@Serializable
@SerialName("TDigest")
data class TDigest(
    val compression: Double = 100.0,
    val probabilities: List<Double> = listOf(0.5, 0.75, 0.9, 0.95, 0.99, 0.999),
) : SeriesStatSpec<TDigestResult>

@Serializable
@SerialName("PitHistogram")
data class PitHistogram(val numBins: Int) : SeriesStatSpec<SparseHistogramResult>

// ========== Paired ==========

@Serializable
@SerialName("PairedSum")
data object PairedSum : PairedStatSpec<PairedSumResult>

@Serializable
@SerialName("OLS")
data object OLS : PairedStatSpec<OLSResult>

@Serializable
@SerialName("Covariance")
data object Covariance : PairedStatSpec<CovarianceResult>

@Serializable
@SerialName("Lasso")
data class Lasso(val lambda: Double) : PairedStatSpec<LassoResult>

@Serializable
@SerialName("Ridge")
data class Ridge(val lambda: Double) : PairedStatSpec<RidgeResult>

@Serializable
@SerialName("BrierScore")
data object BrierScore : PairedStatSpec<WeightedMeanResult>

@Serializable
@SerialName("MseLoss")
data object MseLoss : PairedStatSpec<WeightedMeanResult>

@Serializable
@SerialName("MaeLoss")
data object MaeLoss : PairedStatSpec<WeightedMeanResult>

@Serializable
@SerialName("LogLoss")
data object LogLoss : PairedStatSpec<WeightedMeanResult>

@Serializable
@SerialName("PinballLoss")
data class PinballLoss(val tau: Double) : PairedStatSpec<WeightedMeanResult>

@Serializable
@SerialName("Auc")
data class Auc(
    val numBins: Int = 256,
    val lowerBound: Double = 0.0,
    val upperBound: Double = 1.0,
) : PairedStatSpec<AucResult>

@Serializable
@SerialName("Reliability")
data class Reliability(val numBins: Int) : PairedStatSpec<ReliabilityResult>

// ========== Discrete ==========

@Serializable
@SerialName("HyperLogLog")
data class HyperLogLog(val precision: Int = 14) : DiscreteStatSpec<HyperLogLogResult>

@Serializable
@SerialName("LinearCounting")
data class LinearCounting(val bits: Int = 4096) : DiscreteStatSpec<LinearCountingResult>

@Serializable
@SerialName("BloomFilter")
data class BloomFilter(
    val bits: Int = 1 shl 16,
    val hashes: Int = 7,
) : DiscreteStatSpec<BloomFilterResult>

@Serializable
@SerialName("CountMinSketch")
data class CountMinSketch(
    val depth: Int = 5,
    val width: Int = 1024,
    val seed: Long = -7046029254386353133L,
) : DiscreteStatSpec<CountMinSketchResult>

@Serializable
@SerialName("MinHash")
data class MinHash(
    val numHashes: Int = 128,
    val seed: Long = -3724518991637283867L,
) : DiscreteStatSpec<MinHashResult>

@Serializable
@SerialName("SpaceSaving")
data class SpaceSaving(val capacity: Int) : DiscreteStatSpec<HeavyHittersResult>
