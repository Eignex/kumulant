package com.eignex.kumulant.schema

import com.eignex.kumulant.math.HasherRef
import com.eignex.kumulant.stat.anomaly.FeatureRange
import com.eignex.kumulant.stat.anomaly.GaussianScoreResult
import com.eignex.kumulant.stat.anomaly.HalfSpaceTreesResult
import com.eignex.kumulant.stat.anomaly.QuantileFilterResult
import com.eignex.kumulant.stat.calibration.IsotonicCalibratorResult
import com.eignex.kumulant.stat.calibration.PlattCalibratorResult
import com.eignex.kumulant.stat.calibration.ReliabilityResult
import com.eignex.kumulant.stat.cardinality.HyperLogLogResult
import com.eignex.kumulant.stat.cardinality.LinearCountingResult
import com.eignex.kumulant.stat.change.AdwinResult
import com.eignex.kumulant.stat.change.CusumResult
import com.eignex.kumulant.stat.change.PageHinkleyResult
import com.eignex.kumulant.stat.event.CrossingResult
import com.eignex.kumulant.stat.event.ExcursionResult
import com.eignex.kumulant.stat.event.RecencyResult
import com.eignex.kumulant.stat.event.RunLengthResult
import com.eignex.kumulant.stat.event.SojournResult
import com.eignex.kumulant.stat.quantile.QuantileResult
import com.eignex.kumulant.stat.quantile.ReservoirResult
import com.eignex.kumulant.stat.quantile.SketchResult
import com.eignex.kumulant.stat.quantile.SparseHistogramResult
import com.eignex.kumulant.stat.quantile.TDigestResult
import com.eignex.kumulant.stat.quantile.ThresholdBucketResult
import com.eignex.kumulant.stat.rate.RateResult
import com.eignex.kumulant.stat.regression.CovarianceResult
import com.eignex.kumulant.stat.regression.GaussianNaiveBayesResult
import com.eignex.kumulant.stat.regression.SoftmaxRegressionResult
import com.eignex.kumulant.stat.regression.glm.ConstantRate
import com.eignex.kumulant.stat.regression.glm.CovarianceRegressionResult
import com.eignex.kumulant.stat.regression.glm.DiagonalRegressionResult
import com.eignex.kumulant.stat.regression.glm.Link
import com.eignex.kumulant.stat.regression.glm.Penalty
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionResult
import com.eignex.kumulant.stat.regression.glm.UnivariateRegressionResult
import com.eignex.kumulant.stat.regression.tree.ClassificationTreeConfig
import com.eignex.kumulant.stat.regression.tree.ForestClassificationResult
import com.eignex.kumulant.stat.regression.tree.ForestRegressionResult
import com.eignex.kumulant.stat.regression.tree.RegressionTreeConfig
import com.eignex.kumulant.stat.regression.tree.Split
import com.eignex.kumulant.stat.regression.tree.TreeClassificationResult
import com.eignex.kumulant.stat.regression.tree.TreeRegressionResult
import com.eignex.kumulant.stat.score.AucResult
import com.eignex.kumulant.stat.score.ConfusionMatrixResult
import com.eignex.kumulant.stat.sketch.BloomFilterResult
import com.eignex.kumulant.stat.sketch.CountMinSketchResult
import com.eignex.kumulant.stat.sketch.HeavyHittersResult
import com.eignex.kumulant.stat.sketch.MinHashResult
import com.eignex.kumulant.stat.summary.BernoulliSumResult
import com.eignex.kumulant.stat.summary.MadResult
import com.eignex.kumulant.stat.summary.MaxResult
import com.eignex.kumulant.stat.summary.MinResult
import com.eignex.kumulant.stat.summary.MomentsResult
import com.eignex.kumulant.stat.summary.PairedSumResult
import com.eignex.kumulant.stat.summary.RangeResult
import com.eignex.kumulant.stat.summary.SumResult
import com.eignex.kumulant.stat.summary.SummaryResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

// [StatSpec] variants for kumulant's stats. Co-located here (rather than next to each
// stat) because Kotlin requires direct subclasses of a sealed interface to live in the
// same package as the interface.
//
// Each variant uses `@SerialName` matching its Kotlin class name, so the wire `$type`
// value mirrors what a Kotlin reader would type. Defaults match the underlying stat's
// primary constructor so authored payloads stay terse under `encodeDefaults = false`.
//
// Construction lives in `StatFactory.kt`; specs here are pure data only.

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

/** Spec for `ExcursionStat`: running peak with the largest peak-to-trough excursion observed. */
@Serializable
@SerialName("Excursion")
data object Excursion : SeriesStatSpec<ExcursionResult>

/** Spec for `RunLengthStat`: current and longest consecutive truthy-run lengths. */
@Serializable
@SerialName("RunLength")
data object RunLength : SeriesStatSpec<RunLengthResult>

/** Spec for `RecencyStat`: time elapsed since the most recent observation. */
@Serializable
@SerialName("Recency")
data object Recency : SeriesStatSpec<RecencyResult>

/** Spec for `CrossingStat`: counts upward and downward crossings of a configured level. */
@Serializable
@SerialName("Crossing")
data class Crossing(
    /** The level the input stream is compared against. */
    val level: Double,
) : SeriesStatSpec<CrossingResult>

/** Spec for `CusumStat`: two-sided cumulative-sum change-point detector. */
@Serializable
@SerialName("Cusum")
data class Cusum(
    /** In-control target value to compare each input against. */
    val target: Double = 0.0,
    /** Reference value (allowance) absorbing in-control variation. */
    val referenceValue: Double = 0.5,
    /** Decision threshold for either side. */
    val threshold: Double = 5.0,
) : SeriesStatSpec<CusumResult>

/** Spec for `PageHinkleyStat`: Page-Hinkley change-point detector. */
@Serializable
@SerialName("PageHinkley")
data class PageHinkley(
    /** Tolerance absorbing in-control fluctuation. */
    val delta: Double = 0.005,
    /** Alarm threshold for either drift. */
    val threshold: Double = 50.0,
) : SeriesStatSpec<PageHinkleyResult>

/** Spec for `AdwinStat`: ADWIN2 adaptive-windowing change detector. */
@Serializable
@SerialName("Adwin")
data class Adwin(
    /** Confidence parameter for the Hoeffding-bound cut test. */
    val delta: Double = 0.002,
    /** Maximum number of buckets per power-of-two size class before merging upward. */
    val maxBucketsPerSize: Int = 5,
) : SeriesStatSpec<AdwinResult>

/** Spec for `MadStat`: streaming median and median absolute deviation via two t-digests. */
@Serializable
@SerialName("Mad")
data class Mad(
    /** T-digest compression for both digests; lower = more centroids, tighter quantiles. */
    val compression: Double = 100.0,
) : SeriesStatSpec<MadResult>

/** Spec for `ThresholdBucketStat`: weighted counts per user-defined value bucket. */
@Serializable
@SerialName("ThresholdBucket")
data class ThresholdBucket(
    /** Strictly increasing thresholds defining the bucket edges. */
    val thresholds: List<Double>,
) : SeriesStatSpec<ThresholdBucketResult>

/** Spec for `VarianceStat`: weighted running variance (Welford). */
@Serializable
@SerialName("Variance")
data object Variance : SeriesStatSpec<WeightedVarianceResult>

/** Spec for `MomentsStat`: mean / variance / skewness / kurtosis (Welford). */
@Serializable
@SerialName("Moments")
data object Moments : SeriesStatSpec<MomentsResult>

/** Spec for `GaussianScorerStat`: running mean / variance with `|x - mean| / stdDev` z-score. */
@Serializable
@SerialName("GaussianScorer")
data object GaussianScorer : SeriesStatSpec<GaussianScoreResult>

/** Spec for `QuantileFilterStat`: DDSketch-backed quantile-threshold anomaly detector. */
@Serializable
@SerialName("QuantileFilter")
data class QuantileFilter(
    /** Probability in `(0, 1)` at which the threshold is evaluated. */
    val probability: Double = 0.99,
    /** Relative-error guarantee passed to the underlying DDSketch. */
    val relativeError: Double = 0.01,
) : SeriesStatSpec<QuantileFilterResult>

/** Spec for `SummaryStat`: mean / variance / min / max in one result, useful as a primary
 *  for mixed-scaler feedback projections. */
@Serializable
@SerialName("Summary")
data object Summary : SeriesStatSpec<SummaryResult>

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

/** Spec for `PlattCalibratorStat`: one-feature logistic regression fitting `sigmoid(a*x + b)`. */
@Serializable
@SerialName("PlattCalibrator")
data class PlattCalibrator(
    /** Optimizer driving the underlying logistic regression. */
    val optimizer: OptimizerSpec = Sgd(ConstantRate(1e-2)),
) : PairedStatSpec<PlattCalibratorResult>

/** Spec for `IsotonicCalibratorStat`: binned isotonic calibrator over `[0, 1]`. */
@Serializable
@SerialName("IsotonicCalibrator")
data class IsotonicCalibrator(
    /** Number of equal-width bins covering `[0, 1]`. */
    val numBins: Int = 16,
) : PairedStatSpec<IsotonicCalibratorResult>

/** Spec for `ConfusionMatrixStat`: K-by-K weighted confusion matrix over (predictedClass, trueClass). */
@Serializable
@SerialName("ConfusionMatrix")
data class ConfusionMatrix(
    /** Number of classes; indices are `[0, numClasses)`. */
    val numClasses: Int,
) : PairedStatSpec<ConfusionMatrixResult>

/** Spec for `AccuracyStat`: weighted classification accuracy over (predictedClass, trueClass). */
@Serializable
@SerialName("Accuracy")
data object Accuracy : PairedStatSpec<WeightedMeanResult>

/** Spec for `HyperLogLogStat`: cardinality sketch with controllable [precision]. */
@Serializable
@SerialName("HyperLogLog")
data class HyperLogLog(
    /** Number of register-index bits; memory is `2^precision` bytes. */
    val precision: Int = 14,
    /** [HasherRef] for the mixer applied before bucketing; resolved via the Hashers registry. */
    val hasher: HasherRef = HasherRef.SplitMix64,
) : DiscreteStatSpec<HyperLogLogResult>

/** Spec for `LinearCountingStat`: cardinality estimator backed by a bitset of [bits] cells. */
@Serializable
@SerialName("LinearCounting")
data class LinearCounting(
    /** Bitset size; trade-off between memory and accuracy near the saturation cap. */
    val bits: Int = 4096,
    /** [HasherRef] for the mixer applied before indexing; resolved via the Hashers registry. */
    val hasher: HasherRef = HasherRef.SplitMix64,
) : DiscreteStatSpec<LinearCountingResult>

/** Spec for `BloomFilterStat`: probabilistic set membership. */
@Serializable
@SerialName("BloomFilter")
data class BloomFilter(
    /** Underlying bitset size. */
    val bits: Int = 1 shl 16,
    /** Number of independent hash functions per insert. */
    val hashes: Int = 7,
    /** [HasherRef] for the mixer seeding the double-hashing scheme; resolved via the Hashers registry. */
    val hasher: HasherRef = HasherRef.SplitMix64,
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
    /** [HasherRef] for the mixer applied per row; resolved via the Hashers registry. */
    val hasher: HasherRef = HasherRef.SplitMix64,
) : DiscreteStatSpec<CountMinSketchResult>

/** Spec for `MinHashStat`: Jaccard-similarity signature over [numHashes] independent hash functions. */
@Serializable
@SerialName("MinHash")
data class MinHash(
    /** Signature length; higher means better Jaccard accuracy at more memory. */
    val numHashes: Int = 128,
    /** PRNG seed used to derive the per-hash salts. */
    val seed: Long = -3724518991637283867L,
    /** [HasherRef] for the mixer applied per signature slot; resolved via the Hashers registry. */
    val hasher: HasherRef = HasherRef.SplitMix64,
) : DiscreteStatSpec<MinHashResult>

/** Spec for `SojournStat`: per-state time, transition counts, and current dwell over a declared alphabet. */
@Serializable
@SerialName("Sojourn")
data class Sojourn(
    /** Declared categorical state alphabet, in order. Must be non-empty and unique. */
    val states: List<Long>,
) : DiscreteStatSpec<SojournResult>

/** Spec for `SpaceSavingStat`: top-[capacity] heavy-hitters tracker. */
@Serializable
@SerialName("SpaceSaving")
data class SpaceSaving(
    /** Number of distinct items retained; smaller = more aggressive eviction. */
    val capacity: Int,
) : DiscreteStatSpec<HeavyHittersResult>

/** Spec for `BayesianRegressionStat`: closed-form Gaussian linear regression with isotropic prior. */
@Serializable
@SerialName("BayesianRegression")
data class BayesianRegression(
    /** Number of input features. */
    val featureSize: Int,
    /** Isotropic prior variance applied to every coefficient. */
    val priorVariance: Double = 1.0,
    /** GLM link function; `Link.Identity` keeps the strict closed-form Gaussian posterior. */
    val link: Link = Link.Identity,
) : RegressionStatSpec<CovarianceRegressionResult>

/** Spec for `StochasticRegressionStat`: online GLM with a configurable optimizer. */
@Serializable
@SerialName("StochasticRegression")
data class StochasticRegression(
    /** Number of input features. */
    val featureSize: Int,
    /** Per-coordinate update rule for the weight vector. */
    val optimizer: OptimizerSpec = Sgd(),
    /** Update rule for the bias scalar; defaults to [optimizer]. */
    val biasOptimizer: OptimizerSpec = optimizer,
    /** Gradient-step regulariser. Requires [Sgd] optimizers. */
    val penalty: Penalty = Penalty.None,
    /** GLM link function; `Link.Identity` gives plain OLS. */
    val link: Link = Link.Identity,
) : RegressionStatSpec<StochasticRegressionResult>

/** Spec for `GaussianNaiveBayesStat`: online Gaussian naive Bayes classifier. */
@Serializable
@SerialName("GaussianNaiveBayes")
data class GaussianNaiveBayes(
    /** Number of input features. */
    val featureSize: Int,
    /** Number of classes. */
    val numClasses: Int,
    /** Lower bound applied to per-class variances at predict time. */
    val varianceFloor: Double = 1e-9,
) : RegressionStatSpec<GaussianNaiveBayesResult>

/** Spec for `SoftmaxRegressionStat`: multinomial (K-way) logistic regression. */
@Serializable
@SerialName("SoftmaxRegression")
data class SoftmaxRegression(
    /** Number of input features. */
    val featureSize: Int,
    /** Number of classes; the input `y` must round to `[0, numClasses)`. */
    val numClasses: Int,
    /** Per-class weight-matrix optimizer; one instance is materialised per class. */
    val optimizer: OptimizerSpec = Sgd(),
    /** Bias optimizer; defaults to [optimizer]. */
    val biasOptimizer: OptimizerSpec = optimizer,
) : RegressionStatSpec<SoftmaxRegressionResult>

/** Spec for `DiagonalRegressionStat`: factorised-Gaussian posterior with per-coordinate precision. */
@Serializable
@SerialName("DiagonalRegression")
data class DiagonalRegression(
    /** Number of input features. */
    val featureSize: Int,
    /** Initial per-coordinate precision (inverse variance) seeded into every weight. */
    val priorPrecision: Double = 1.0,
    /** Per-step learning rate. */
    val learningRate: ScalarExpr = ConstantRate(1.0),
    /** Gradient-step regulariser. */
    val penalty: Penalty = Penalty.None,
    /** GLM link function. */
    val link: Link = Link.Identity,
) : RegressionStatSpec<DiagonalRegressionResult>

/** Spec for `DecisionTreeRegressionStat`: online VFDT regression tree. */
@Serializable
@SerialName("DecisionTreeRegression")
data class DecisionTreeRegression(
    /** Number of input features. */
    val featureSize: Int,
    /** Candidate splits considered at every audit leaf. */
    val splitCandidates: List<Split>,
    /** RegressionTree growth tunables. */
    val config: RegressionTreeConfig = RegressionTreeConfig(),
    /** PRNG seed for per-leaf candidate subsampling and bagging. */
    val randomSeed: Int = 0,
) : RegressionStatSpec<TreeRegressionResult>

/** Spec for `RandomForestRegressionStat`: ensembled VFDT regression forest. */
@Serializable
@SerialName("RandomForestRegression")
data class RandomForestRegression(
    /** Number of input features. */
    val featureSize: Int,
    /** Candidate split pool. */
    val splitCandidates: List<Split>,
    /** Trees in the forest. */
    val nbrTrees: Int = 10,
    /** RegressionTree growth tunables (mtry defaults to `ceil(sqrt(p))` when null). */
    val config: RegressionTreeConfig = RegressionTreeConfig(),
    /** Oza & Russell Poisson(1) per-tree reweighting. */
    val bagging: Boolean = true,
    /** PRNG seed shared across trees. */
    val randomSeed: Int = 0,
) : RegressionStatSpec<ForestRegressionResult>

/** Spec for `DecisionTreeClassifierStat`: online VFDT classification tree. */
@Serializable
@SerialName("DecisionTreeClassifier")
data class DecisionTreeClassifier(
    /** Number of input features. */
    val featureSize: Int,
    /** Number of classes; `y` must round to `[0, numClasses)`. */
    val numClasses: Int,
    /** Candidate splits considered at every audit leaf. */
    val splitCandidates: List<Split>,
    /** RegressionTree growth tunables. */
    val config: ClassificationTreeConfig = ClassificationTreeConfig(),
    /** PRNG seed. */
    val randomSeed: Int = 0,
) : RegressionStatSpec<TreeClassificationResult>

/** Spec for `RandomForestClassifierStat`: ensembled VFDT classification forest. */
@Serializable
@SerialName("RandomForestClassifier")
data class RandomForestClassifier(
    /** Number of input features. */
    val featureSize: Int,
    /** Number of classes. */
    val numClasses: Int,
    /** Candidate split pool. */
    val splitCandidates: List<Split>,
    /** Trees in the forest. */
    val nbrTrees: Int = 10,
    /** RegressionTree growth tunables (mtry defaults to `ceil(sqrt(p))` when null). */
    val config: ClassificationTreeConfig = ClassificationTreeConfig(),
    /** Oza & Russell Poisson(1) per-tree reweighting. */
    val bagging: Boolean = true,
    /** PRNG seed shared across trees. */
    val randomSeed: Int = 0,
) : RegressionStatSpec<ForestClassificationResult>

/** Spec for `HalfSpaceTreesStat`: online ensemble of random half-space trees for multivariate anomaly scoring. */
@Serializable
@SerialName("HalfSpaceTrees")
data class HalfSpaceTrees(
    /** Number of input features. */
    val featureSize: Int,
    /** Per-feature value ranges used to draw random split thresholds. */
    val featureRanges: List<FeatureRange>,
    /** Number of trees in the ensemble. */
    val numTrees: Int = 25,
    /** Depth of each tree; each tree has `2^height` leaves. */
    val height: Int = 8,
    /** Observations per window before the reference profile rotates. */
    val windowSize: Int = 250,
    /** PRNG seed shared across trees. */
    val randomSeed: Int = 0,
) : VectorStatSpec<HalfSpaceTreesResult>
