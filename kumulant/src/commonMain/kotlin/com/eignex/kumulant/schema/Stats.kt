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
import com.eignex.kumulant.stat.regression.Penalty
import com.eignex.kumulant.stat.regression.UnivariateRegressionResult
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

/** Spec for `MeanStat`: weighted running mean. */
@Serializable
@SerialName("Mean")
data object Mean : SeriesStatSpec<WeightedMeanResult>

/** Spec for `SumStat`: weighted running sum. */
@Serializable
@SerialName("Sum")
data object Sum : SeriesStatSpec<SumResult>

/** Spec for `MinStat`: running minimum. */
@Serializable
@SerialName("Min")
data object Min : SeriesStatSpec<MinResult>

/** Spec for `MaxStat`: running maximum. */
@Serializable
@SerialName("Max")
data object Max : SeriesStatSpec<MaxResult>

/** Spec for `RangeStat`: running min and max as a pair. */
@Serializable
@SerialName("Range")
data object Range : SeriesStatSpec<RangeResult>

/** Spec for `VarianceStat`: weighted running variance (Welford). */
@Serializable
@SerialName("Variance")
data object Variance : SeriesStatSpec<WeightedVarianceResult>

/** Spec for `MomentsStat`: mean / variance / skewness / kurtosis (Welford). */
@Serializable
@SerialName("Moments")
data object Moments : SeriesStatSpec<MomentsResult>

/** Spec for `BernoulliSumStat`: weighted count of nonzero inputs. */
@Serializable
@SerialName("BernoulliSum")
data object BernoulliSum : SeriesStatSpec<BernoulliSumResult>

/** Spec for `TotalWeightsStat`: cumulative observation weight. */
@Serializable
@SerialName("TotalWeights")
data object TotalWeights : SeriesStatSpec<SumResult>

/** Spec for `CountStat`: unweighted observation count. */
@Serializable
@SerialName("Count")
data object Count : SeriesStatSpec<SumResult>

/** Spec for `RateStat`: events per second over the observed wall-clock span. */
@Serializable
@SerialName("Rate")
data object Rate : SeriesStatSpec<RateResult>

/** Spec for `CounterRateStat`: rate inferred from a monotonically increasing counter. */
@Serializable
@SerialName("CounterRate")
data class CounterRate(
    /** When true, a decrease in the counter is interpreted as a reset rather than a negative rate. */
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
    /** Relative error guarantee on the returned quantiles. */
    val relativeError: Double = 0.01,
    /** Quantiles to evaluate at read time. */
    val probabilities: List<Double> = listOf(0.5, 0.75, 0.9, 0.95, 0.99, 0.999),
) : SeriesStatSpec<SketchResult>

/** Spec for `FrugalQuantileStat`: O(1)-memory single-quantile estimator. */
@Serializable
@SerialName("FrugalQuantile")
data class FrugalQuantile(
    /** Target quantile probability in `(0, 1)`. */
    val q: Double,
    /** Adaptive step size used to chase the quantile. */
    val stepSize: Double = 0.01,
    /** Initial estimate seeding the random walk. */
    val initialEstimate: Double = 0.0,
) : SeriesStatSpec<QuantileResult>

/** Spec for `HdrHistogramStat`: high-dynamic-range histogram. */
@Serializable
@SerialName("HdrHistogram")
data class HdrHistogram(
    /** Smallest value the histogram can distinguish. */
    val lowestDiscernibleValue: Double = 0.001,
    /** Initial upper bound; the histogram grows past this if needed. */
    val initialHighestTrackableValue: Double = 100.0,
    /** Number of significant digits of precision (1..5). */
    val significantDigits: Int = 3,
) : SeriesStatSpec<SparseHistogramResult>

/** Spec for `LinearHistogramStat`: fixed-width bins over `[lowerBound, upperBound)`. */
@Serializable
@SerialName("LinearHistogram")
data class LinearHistogram(
    /** Inclusive lower bound of the histogram's covered range. */
    val lowerBound: Double,
    /** Exclusive upper bound of the histogram's covered range. */
    val upperBound: Double,
    /** Number of equal-width bins between the bounds. */
    val binCount: Int,
) : SeriesStatSpec<SparseHistogramResult>

/**
 * Configuration for `ReservoirHistogramStat`. Seed has no default - the live constructor's
 * `Random.Default.nextLong()` is non-deterministic, which would silently produce
 * different goldens on each instantiation if mirrored here.
 */
@Serializable
@SerialName("ReservoirHistogram")
data class ReservoirHistogram(
    /** Reservoir size (capacity of retained samples). */
    val capacity: Int = 1024,
    /** PRNG seed for reproducible reservoir admission. */
    val seed: Long,
) : SeriesStatSpec<ReservoirResult>

/** Spec for `TDigestStat`: streaming t-digest quantile sketch. */
@Serializable
@SerialName("TDigest")
data class TDigest(
    /** Compression parameter; lower = more clusters, tighter quantiles, more memory. */
    val compression: Double = 100.0,
    /** Quantiles to evaluate at read time. */
    val probabilities: List<Double> = listOf(0.5, 0.75, 0.9, 0.95, 0.99, 0.999),
) : SeriesStatSpec<TDigestResult>

/** Spec for `pitHistogram(numBins)`: PIT-style equiprobable histogram for calibration checks. */
@Serializable
@SerialName("PitHistogram")
data class PitHistogram(
    /** Number of equiprobable bins. */
    val numBins: Int,
) : SeriesStatSpec<SparseHistogramResult>

/** Spec for `PairedSumStat`: tracks per-axis sums of `(x, y)` updates. */
@Serializable
@SerialName("PairedSum")
data object PairedSum : PairedStatSpec<PairedSumResult>

/** Spec for `UnivariateRegressionStat`: scalar OLS / Lasso / Ridge depending on [penalty]. */
@Serializable
@SerialName("UnivariateRegression")
data class UnivariateRegression(
    /** Regularisation applied at `read()` time; defaults to plain OLS. */
    val penalty: Penalty = Penalty.None,
) : PairedStatSpec<UnivariateRegressionResult>

/** Spec for `CovarianceStat`: weighted covariance and Pearson correlation between two streams. */
@Serializable
@SerialName("Covariance")
data object Covariance : PairedStatSpec<CovarianceResult>

/** Spec for `BrierScoreStat`: mean squared error against `y in {0, 1}` for probabilistic predictions. */
@Serializable
@SerialName("BrierScore")
data object BrierScore : PairedStatSpec<WeightedMeanResult>

/** Spec for `MseLossStat`: mean squared error `(y - yhat)^2`. */
@Serializable
@SerialName("MseLoss")
data object MseLoss : PairedStatSpec<WeightedMeanResult>

/** Spec for `MaeLossStat`: mean absolute error `|y - yhat|`. */
@Serializable
@SerialName("MaeLoss")
data object MaeLoss : PairedStatSpec<WeightedMeanResult>

/** Spec for `LogLossStat`: mean negative log-likelihood for binary `y` with predicted probability `x`. */
@Serializable
@SerialName("LogLoss")
data object LogLoss : PairedStatSpec<WeightedMeanResult>

/** Spec for `PinballLossStat`: quantile (pinball) loss at quantile [tau]. */
@Serializable
@SerialName("PinballLoss")
data class PinballLoss(
    /** Target quantile in `(0, 1)`. */
    val tau: Double,
) : PairedStatSpec<WeightedMeanResult>

/** Spec for `AucStat`: streaming AUC over a fixed-resolution score histogram. */
@Serializable
@SerialName("Auc")
data class Auc(
    /** Number of histogram bins covering `[lowerBound, upperBound]`. */
    val numBins: Int = 256,
    /** Inclusive lower bound on the score range. */
    val lowerBound: Double = 0.0,
    /** Inclusive upper bound on the score range. */
    val upperBound: Double = 1.0,
) : PairedStatSpec<AucResult>

/** Spec for `ReliabilityStat`: per-bin calibration table (mean predicted vs observed frequency). */
@Serializable
@SerialName("Reliability")
data class Reliability(
    /** Number of equal-width probability bins. */
    val numBins: Int,
) : PairedStatSpec<ReliabilityResult>

/** Spec for `HyperLogLogStat`: cardinality sketch with controllable [precision]. */
@Serializable
@SerialName("HyperLogLog")
data class HyperLogLog(
    /** Number of register-index bits; memory is `2^precision` bytes. */
    val precision: Int = 14,
) : DiscreteStatSpec<HyperLogLogResult>

/** Spec for `LinearCountingStat`: cardinality estimator backed by a bitset of [bits] cells. */
@Serializable
@SerialName("LinearCounting")
data class LinearCounting(
    /** Bitset size; trade-off between memory and accuracy near the saturation cap. */
    val bits: Int = 4096,
) : DiscreteStatSpec<LinearCountingResult>

/** Spec for `BloomFilterStat`: probabilistic set membership. */
@Serializable
@SerialName("BloomFilter")
data class BloomFilter(
    /** Underlying bitset size. */
    val bits: Int = 1 shl 16,
    /** Number of independent hash functions per insert. */
    val hashes: Int = 7,
) : DiscreteStatSpec<BloomFilterResult>

/** Spec for `CountMinSketchStat`: approximate frequency table for unbounded-cardinality streams. */
@Serializable
@SerialName("CountMinSketch")
data class CountMinSketch(
    /** Number of independent hash rows. */
    val depth: Int = 5,
    /** Number of counter columns per row. */
    val width: Int = 1024,
    /** PRNG seed used to derive the per-row hash salts. */
    val seed: Long = -7046029254386353133L,
) : DiscreteStatSpec<CountMinSketchResult>

/** Spec for `MinHashStat`: Jaccard-similarity signature over [numHashes] independent hash functions. */
@Serializable
@SerialName("MinHash")
data class MinHash(
    /** Signature length; higher means better Jaccard accuracy at more memory. */
    val numHashes: Int = 128,
    /** PRNG seed used to derive the per-hash salts. */
    val seed: Long = -3724518991637283867L,
) : DiscreteStatSpec<MinHashResult>

/** Spec for `SpaceSavingStat`: top-[capacity] heavy-hitters tracker. */
@Serializable
@SerialName("SpaceSaving")
data class SpaceSaving(
    /** Number of distinct items retained; smaller = more aggressive eviction. */
    val capacity: Int,
) : DiscreteStatSpec<HeavyHittersResult>
