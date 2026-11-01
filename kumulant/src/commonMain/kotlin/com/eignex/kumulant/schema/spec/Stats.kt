package com.eignex.kumulant.schema.spec

import com.eignex.koblas.ContiguousVector
import com.eignex.koblas.DenseMatrix
import com.eignex.kumulant.math.HasherRef
import com.eignex.kumulant.schema.expr.*
import com.eignex.kumulant.schema.optimizer.*
import com.eignex.kumulant.stat.StatDefaults
import com.eignex.kumulant.stat.calibration.PlattCalibratorResult
import com.eignex.kumulant.stat.calibration.ReliabilityResult
import com.eignex.kumulant.stat.cardinality.HyperLogLogResult
import com.eignex.kumulant.stat.change.AdwinResult
import com.eignex.kumulant.stat.change.CusumResult
import com.eignex.kumulant.stat.change.PageHinkleyResult
import com.eignex.kumulant.stat.event.CrossingResult
import com.eignex.kumulant.stat.event.RecencyResult
import com.eignex.kumulant.stat.event.RunLengthResult
import com.eignex.kumulant.stat.event.SojournResult
import com.eignex.kumulant.stat.quantile.QuantileResult
import com.eignex.kumulant.stat.quantile.ReservoirResult
import com.eignex.kumulant.stat.quantile.SketchResult
import com.eignex.kumulant.stat.quantile.SparseHistogramResult
import com.eignex.kumulant.stat.quantile.TDigestResult
import com.eignex.kumulant.stat.rate.RateResult
import com.eignex.kumulant.stat.regression.CovarianceResult
import com.eignex.kumulant.stat.regression.GaussianNaiveBayesResult
import com.eignex.kumulant.stat.regression.SoftmaxRegressionResult
import com.eignex.kumulant.stat.regression.glm.DiagonalRegressionResult
import com.eignex.kumulant.stat.regression.glm.Link
import com.eignex.kumulant.stat.regression.glm.Penalty
import com.eignex.kumulant.stat.regression.glm.PrecisionRegressionResult
import com.eignex.kumulant.stat.regression.glm.StochasticRegressionResult
import com.eignex.kumulant.stat.regression.glm.UnivariateRegressionResult
import com.eignex.kumulant.stat.regression.tree.ClassCountsResult
import com.eignex.kumulant.stat.regression.tree.ClassificationTreeConfig
import com.eignex.kumulant.stat.regression.tree.ForestClassificationResult
import com.eignex.kumulant.stat.regression.tree.ForestRegressionResult
import com.eignex.kumulant.stat.regression.tree.RegressionTreeConfig
import com.eignex.kumulant.stat.regression.tree.SerializableSplit
import com.eignex.kumulant.stat.regression.tree.TreeClassificationResult
import com.eignex.kumulant.stat.regression.tree.TreeRegressionResult
import com.eignex.kumulant.stat.score.AucResult
import com.eignex.kumulant.stat.score.ConfusionMatrixResult
import com.eignex.kumulant.stat.sketch.BloomFilterResult
import com.eignex.kumulant.stat.sketch.CountMinSketchResult
import com.eignex.kumulant.stat.sketch.HeavyHittersResult
import com.eignex.kumulant.stat.summary.ArgMaxResult
import com.eignex.kumulant.stat.summary.ArgMinResult
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

// Direct subclasses share the sealed interface package. Defaults match stat constructors for terse payloads.
// Serial names preserve tags for non-JSON formats.

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

/** Spec for `ArgMinStat`: running minimum plus the timestamp at which it occurred. */
@Serializable
@SerialName("ArgMin")
data object ArgMin : SeriesStatSpec<ArgMinResult>

/** Spec for `ArgMaxStat`: running maximum plus the timestamp at which it occurred. */
@Serializable
@SerialName("ArgMax")
data object ArgMax : SeriesStatSpec<ArgMaxResult>

/** Spec for `RangeStat`: running min and max as a pair. */
@Serializable
@SerialName("Range")
data object Range : SeriesStatSpec<RangeResult>

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
    val target: Double = StatDefaults.CUSUM_TARGET,
    /** Reference value (allowance) absorbing in-control variation. */
    val referenceValue: Double = StatDefaults.CUSUM_REFERENCE_VALUE,
    /** Decision threshold for either side. */
    val threshold: Double = StatDefaults.CUSUM_THRESHOLD,
) : SeriesStatSpec<CusumResult>

/** Spec for `PageHinkleyStat`: Page-Hinkley change-point detector. */
@Serializable
@SerialName("PageHinkley")
data class PageHinkley(
    /** Tolerance absorbing in-control fluctuation. */
    val delta: Double = StatDefaults.PAGE_HINKLEY_DELTA,
    /** Alarm threshold for either drift. */
    val threshold: Double = StatDefaults.PAGE_HINKLEY_THRESHOLD,
) : SeriesStatSpec<PageHinkleyResult>

/** Spec for `AdwinStat`: ADWIN2 adaptive-windowing change detector. */
@Serializable
@SerialName("Adwin")
data class Adwin(
    /** Confidence parameter for the Hoeffding-bound cut test. */
    val delta: Double = StatDefaults.ADWIN_DELTA,
    /** Maximum number of buckets per power-of-two size class before merging upward. */
    val maxBucketsPerSize: Int = StatDefaults.ADWIN_MAX_BUCKETS_PER_SIZE,
) : SeriesStatSpec<AdwinResult>

/** Spec for `MadStat`: streaming median and median absolute deviation from one raw-value t-digest. */
@Serializable
@SerialName("Mad")
data class Mad(
    /** T-digest compression; larger values retain more centroids and finer detail. */
    val compression: Double = StatDefaults.TDIGEST_COMPRESSION,
) : SeriesStatSpec<MadResult>

/** Spec for `FrugalMadStat`: constant-memory adaptive median and MAD estimates. */
@Serializable
@SerialName("FrugalMad")
data class FrugalMad(
    /** Full step size in input units; each adjustment is bounded by half this value. */
    val stepSize: Double = StatDefaults.FRUGAL_QUANTILE_STEP_SIZE,
) : SeriesStatSpec<MadResult>

/**
 * Spec for `BucketHistogramStat`: weighted counts over caller-chosen bucket edges, each bucket
 * including its upper edge. Build evenly spaced edges with `linearEdges`.
 */
@Serializable
@SerialName("BucketHistogram")
data class BucketHistogram(
    /** Strictly increasing, finite bucket edges. */
    val edges: List<Double>,
) : SeriesStatSpec<SparseHistogramResult>

/** Spec for `VarianceStat`: weighted running variance (Welford). */
@Serializable
@SerialName("Variance")
data object Variance : SeriesStatSpec<WeightedVarianceResult>

/** Spec for `MomentsStat`: mean / variance / skewness / kurtosis (Welford). */
@Serializable
@SerialName("Moments")
data object Moments : SeriesStatSpec<MomentsResult>

/** Spec for `ClassCountsStat`: weighted count per class over a discrete label stream. */
@Serializable
@SerialName("ClassCounts")
data class ClassCounts(
    /** Number of classes; each observed value must be exactly an integer in `[0, numClasses)`. */
    val numClasses: Int,
) : SeriesStatSpec<ClassCountsResult>

/** Spec for `SummaryStat`: mean / variance / min / max in one result, useful as a primary
 *  for mixed-scaler feedback projections. */
@Serializable
@SerialName("Summary")
data object Summary : SeriesStatSpec<SummaryResult>

/** Spec for `BernoulliSumStat`: weighted count of nonzero inputs. */
@Serializable
@SerialName("BernoulliSum")
data object BernoulliSum : SeriesStatSpec<BernoulliSumResult>

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
    val treatDecreaseAsReset: Boolean = StatDefaults.COUNTER_RATE_TREAT_DECREASE_AS_RESET,
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
    val relativeError: Double = StatDefaults.DD_SKETCH_RELATIVE_ERROR,
    /** Quantiles to evaluate at read time. */
    val probabilities: List<Double> = StatDefaults.quantileProbabilities().toList(),
    /** Smallest magnitude given its own bin; smaller non-zero values fold into the bottom bin. */
    val minIndexableValue: Double = StatDefaults.DD_SKETCH_MIN_INDEXABLE_VALUE,
    /** Largest magnitude given its own bin; larger values fold into the top bin. */
    val maxIndexableValue: Double = StatDefaults.DD_SKETCH_MAX_INDEXABLE_VALUE,
) : SeriesStatSpec<SketchResult>

/** Spec for `FrugalQuantileStat`: O(1)-memory single-quantile estimator. */
@Serializable
@SerialName("FrugalQuantile")
data class FrugalQuantile(
    /** Target quantile probability in `(0, 1)`. */
    val q: Double,
    /** Adaptive step size used to chase the quantile. */
    val stepSize: Double = StatDefaults.FRUGAL_QUANTILE_STEP_SIZE,
    /** Initial estimate seeding the random walk. */
    val initialEstimate: Double = StatDefaults.FRUGAL_QUANTILE_INITIAL_ESTIMATE,
) : SeriesStatSpec<QuantileResult>

/**
 * Configuration for `ReservoirHistogramStat`. Seed has no default - the live constructor's
 * `Random.Default.nextLong()` is non-deterministic, which would silently produce
 * different goldens on each instantiation if mirrored here.
 */
@Serializable
@SerialName("ReservoirHistogram")
data class ReservoirHistogram(
    /** Reservoir size (capacity of retained samples). */
    val capacity: Int = StatDefaults.RESERVOIR_HISTOGRAM_CAPACITY,
    /** PRNG seed for reproducible reservoir admission. */
    val seed: Long,
) : SeriesStatSpec<ReservoirResult>

/** Spec for `TDigestStat`: streaming t-digest quantile sketch. */
@Serializable
@SerialName("TDigest")
data class TDigest(
    /** Compression parameter; lower = more clusters, tighter quantiles, more memory. */
    val compression: Double = StatDefaults.TDIGEST_COMPRESSION,
    /** Quantiles to evaluate at read time. */
    val probabilities: List<Double> = StatDefaults.quantileProbabilities().toList(),
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
    val penalty: Penalty = StatDefaults.regressionPenalty,
) : PairedStatSpec<UnivariateRegressionResult>

/** Spec for `CovarianceStat`: weighted covariance and Pearson correlation between two streams. */
@Serializable
@SerialName("Covariance")
data object Covariance : PairedStatSpec<CovarianceResult>

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
    val numBins: Int = StatDefaults.AUC_NUM_BINS,
    /** Inclusive lower bound on the score range. */
    val lowerBound: Double = StatDefaults.AUC_LOWER_BOUND,
    /** Inclusive upper bound on the score range. */
    val upperBound: Double = StatDefaults.AUC_UPPER_BOUND,
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
    val optimizer: OptimizerSpec = StatDefaults.plattOptimizer(),
) : PairedStatSpec<PlattCalibratorResult>

/** Spec for `ConfusionMatrixStat`: K-by-K weighted confusion matrix over (predictedClass, trueClass). */
@Serializable
@SerialName("ConfusionMatrix")
data class ConfusionMatrix(
    /** Number of classes; indices are `[0, numClasses)`. */
    val numClasses: Int,
) : PairedStatSpec<ConfusionMatrixResult>

/**
 * Spec for `AccuracyStat`: weighted classification accuracy over (predictedClass, trueClass).
 *
 * [numClasses] is required rather than defaulted: accuracy is only comparable with `ConfusionMatrix` over
 * the same stream if both bound the labels the same way, and defaulting a bound would silently score
 * labels the author never declared.
 */
@Serializable
@SerialName("Accuracy")
data class Accuracy(
    /** Number of classes; class indices are `[0, numClasses)`. */
    val numClasses: Int,
) : PairedStatSpec<WeightedMeanResult>

/** Spec for `HyperLogLogStat`: cardinality sketch with controllable [precision]. */
@Serializable
@SerialName("HyperLogLog")
data class HyperLogLog(
    /** Number of register-index bits; memory is `2^precision` bytes. */
    val precision: Int = StatDefaults.HYPER_LOG_LOG_PRECISION,
    /** [HasherRef] for the mixer applied before bucketing; resolved via the Hashers registry. */
    val hasher: HasherRef = HasherRef.SplitMix64,
) : DiscreteStatSpec<HyperLogLogResult>

/** Spec for `BloomFilterStat`: probabilistic set membership. */
@Serializable
@SerialName("BloomFilter")
data class BloomFilter(
    /** Underlying bitset size. */
    val bits: Int = StatDefaults.BLOOM_FILTER_BITS,
    /** Number of independent hash functions per insert. */
    val hashes: Int = StatDefaults.BLOOM_FILTER_HASHES,
    /** [HasherRef] for the mixer seeding the double-hashing scheme; resolved via the Hashers registry. */
    val hasher: HasherRef = HasherRef.SplitMix64,
) : DiscreteStatSpec<BloomFilterResult>

/** Spec for `CountMinSketchStat`: approximate frequency table for unbounded-cardinality streams. */
@Serializable
@SerialName("CountMinSketch")
data class CountMinSketch(
    /** Number of independent hash rows. */
    val depth: Int = StatDefaults.COUNT_MIN_SKETCH_DEPTH,
    /** Number of counter columns per row. */
    val width: Int = StatDefaults.COUNT_MIN_SKETCH_WIDTH,
    /** PRNG seed used to derive the per-row hash salts. */
    val seed: Long = StatDefaults.COUNT_MIN_SKETCH_SEED,
    /** [HasherRef] for the mixer applied per row; resolved via the Hashers registry. */
    val hasher: HasherRef = HasherRef.SplitMix64,
) : DiscreteStatSpec<CountMinSketchResult>

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

/** Spec for `BayesianRegressionStat`: Gaussian linear regression with a configurable prior. */
@Serializable
@SerialName("BayesianRegression")
data class BayesianRegression(
    /** Number of input features. */
    val featureSize: Int,
    /** Isotropic prior variance applied to every coefficient. */
    val priorVariance: Double = StatDefaults.BAYESIAN_REGRESSION_PRIOR_VARIANCE,
    /** GLM link function; `Link.Identity` keeps the strict closed-form Gaussian posterior. */
    val link: Link = StatDefaults.regressionLink,
    /** Prior coefficient mean; null uses the zero vector. */
    val priorMean: ContiguousVector? = null,
    /** Prior coefficient covariance; null uses [priorVariance] times the identity. */
    val priorCovariance: DenseMatrix? = null,
) : RegressionStatSpec<PrecisionRegressionResult>

/** Spec for `StochasticRegressionStat`: online GLM with a configurable optimizer. */
@Serializable
@SerialName("StochasticRegression")
data class StochasticRegression(
    /** Number of input features. */
    val featureSize: Int,
    /** Per-coordinate update rule for the weight vector. */
    val optimizer: OptimizerSpec = StatDefaults.regressionOptimizer(),
    /** Update rule for the bias scalar; defaults to [optimizer]. */
    val biasOptimizer: OptimizerSpec = optimizer,
    /** Gradient-step regulariser. Requires [Sgd] optimizers. */
    val penalty: Penalty = StatDefaults.regressionPenalty,
    /** GLM link function; `Link.Identity` gives plain OLS. */
    val link: Link = StatDefaults.regressionLink,
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
    val varianceFloor: Double = StatDefaults.GAUSSIAN_NAIVE_BAYES_VARIANCE_FLOOR,
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
    val optimizer: OptimizerSpec = StatDefaults.regressionOptimizer(),
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
    val priorPrecision: Double = StatDefaults.DIAGONAL_REGRESSION_PRIOR_PRECISION,
    /** Per-step learning rate. */
    val learningRate: ScalarExpr = StatDefaults.diagonalLearningRate(),
    /** Gradient-step regulariser. */
    val penalty: Penalty = StatDefaults.regressionPenalty,
    /** GLM link function. */
    val link: Link = StatDefaults.regressionLink,
) : RegressionStatSpec<DiagonalRegressionResult>

/** Spec for `DecisionTreeRegressionStat`: online VFDT regression tree. */
@Serializable
@SerialName("DecisionTreeRegression")
data class DecisionTreeRegression(
    /** Number of input features. */
    val featureSize: Int,
    /** Candidate splits considered at every audit leaf. */
    val splitCandidates: List<SerializableSplit>,
    /** RegressionTree growth tunables. */
    val config: RegressionTreeConfig = RegressionTreeConfig(),
    /** PRNG seed for per-leaf candidate subsampling and bagging. */
    val randomSeed: Int = StatDefaults.TREE_RANDOM_SEED,
    /** Per-leaf accumulator recipe; null uses the stat's default leaf arm. */
    val leafArm: SeriesStatSpec<WeightedVarianceResult>? = null,
) : RegressionStatSpec<TreeRegressionResult>

/** Spec for `RandomForestRegressionStat`: ensembled VFDT regression forest. */
@Serializable
@SerialName("RandomForestRegression")
data class RandomForestRegression(
    /** Number of input features. */
    val featureSize: Int,
    /** Candidate split pool. */
    val splitCandidates: List<SerializableSplit>,
    /** Trees in the forest. */
    val nbrTrees: Int = StatDefaults.FOREST_SIZE,
    /** RegressionTree growth tunables (mtry defaults to `ceil(sqrt(p))` when null). */
    val config: RegressionTreeConfig = RegressionTreeConfig(),
    /** Oza & Russell Poisson(1) per-tree reweighting. */
    val bagging: Boolean = StatDefaults.FOREST_BAGGING,
    /** PRNG seed shared across trees. */
    val randomSeed: Int = StatDefaults.TREE_RANDOM_SEED,
    /** Per-leaf accumulator recipe; null uses the stat's default leaf arm. */
    val leafArm: SeriesStatSpec<WeightedVarianceResult>? = null,
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
    val splitCandidates: List<SerializableSplit>,
    /** RegressionTree growth tunables. */
    val config: ClassificationTreeConfig = ClassificationTreeConfig(),
    /** PRNG seed. */
    val randomSeed: Int = StatDefaults.TREE_RANDOM_SEED,
    /** Per-leaf accumulator recipe; null uses the stat's default leaf arm. */
    val leafArm: SeriesStatSpec<ClassCountsResult>? = null,
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
    val splitCandidates: List<SerializableSplit>,
    /** Trees in the forest. */
    val nbrTrees: Int = StatDefaults.FOREST_SIZE,
    /** RegressionTree growth tunables (mtry defaults to `ceil(sqrt(p))` when null). */
    val config: ClassificationTreeConfig = ClassificationTreeConfig(),
    /** Oza & Russell Poisson(1) per-tree reweighting. */
    val bagging: Boolean = StatDefaults.FOREST_BAGGING,
    /** PRNG seed shared across trees. */
    val randomSeed: Int = StatDefaults.TREE_RANDOM_SEED,
    /** Per-leaf accumulator recipe; null uses the stat's default leaf arm. */
    val leafArm: SeriesStatSpec<ClassCountsResult>? = null,
) : RegressionStatSpec<ForestClassificationResult>
