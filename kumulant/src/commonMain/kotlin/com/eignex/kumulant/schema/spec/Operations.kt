package com.eignex.kumulant.schema.spec

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.schema.BandResult
import com.eignex.kumulant.schema.expr.*
import com.eignex.kumulant.stream.DEFAULT_TARGET_HIGH
import com.eignex.kumulant.stream.DEFAULT_TARGET_LOW
import com.eignex.kumulant.stream.DEFAULT_WINDOW_SLICES
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

// Wire-friendly counterparts of the composable operations in
// `com.eignex.kumulant.operation.*`. Each spec holds an inner [StatSpec]
// (polymorphic by `@SerialName`) plus the operation's primitive parameters.
//
// The inner field is typed at the base [StatSpec] interface so the
// polymorphic decoder doesn't need a generic argument; construction
// (`StatFactory.kt`) runtime-checks the inner's modality and casts it. The
// user-facing typed surface is the extension functions below on the
// modality-specific spec interfaces (`SeriesStatSpec<R>.withWeight(...)`,
// etc.) - the unsafe cast there is correct because the wrapper is
// parametric in `R` only at the type level.
//
// AST-backed operations (filter, transform, weightBy, fold) reach into
// `ScalarExpr` / `BoolExpr` / `VectorExpr` so the whole composition serialises.
// Lambda-only live forms (`mapResult`, the lambda overloads of
// `transformValue`/`transformPair`/`filter`) stay in the live-stat package only.

/** Wire spec for `SeriesStat.withWeight(weight)`: multiplies every update by [weight]. */
@Serializable
@SerialName("WithWeightSeries")
internal data class WithWeightSeries(
    /** Inner spec whose updates are weighted. */
    val inner: StatSpec,
    /** Per-update weight multiplier. */
    val weight: Double,
) : SeriesStatSpec<Result>

/** Wire spec for `PairedStat.withWeight(weight)`: multiplies every update by [weight]. */
@Serializable
@SerialName("WithWeightPaired")
internal data class WithWeightPaired(
    /** Inner spec whose updates are weighted. */
    val inner: StatSpec,
    /** Per-update weight multiplier. */
    val weight: Double,
) : PairedStatSpec<Result>

/** Wire spec for `VectorStat.withWeight(weight)`: multiplies every update by [weight]. */
@Serializable
@SerialName("WithWeightVector")
internal data class WithWeightVector(
    /** Inner spec whose updates are weighted. */
    val inner: StatSpec,
    /** Per-update weight multiplier. */
    val weight: Double,
) : VectorStatSpec<Result>

/** Wire spec for `DiscreteStat.withWeight(weight)`: multiplies every update by [weight]. */
@Serializable
@SerialName("WithWeightDiscrete")
internal data class WithWeightDiscrete(
    /** Inner spec whose updates are weighted. */
    val inner: StatSpec,
    /** Per-update weight multiplier. */
    val weight: Double,
) : DiscreteStatSpec<Result>

/** Wire spec for `SeriesStat.withValue(value)`: pins every update to [value]. */
@Serializable
@SerialName("WithValueSeries")
internal data class WithValueSeries(
    /** Inner spec whose updates use the fixed [value]. */
    val inner: StatSpec,
    /** Value pushed into the inner stat on every update. */
    val value: Double,
) : SeriesStatSpec<Result>

/** Wire spec for `DiscreteStat.withValue(value)`: pins every update to [value]. */
@Serializable
@SerialName("WithValueDiscrete")
internal data class WithValueDiscrete(
    /** Inner spec whose updates use the fixed [value]. */
    val inner: StatSpec,
    /** Value pushed into the inner stat on every update. */
    val value: Long,
) : DiscreteStatSpec<Result>

/** Wire spec for `DiscreteStat.asSeries()`: views a discrete stat as a series stat. */
@Serializable
@SerialName("AsSeries")
internal data class AsSeries(
    /** Inner discrete spec being adapted to the series modality. */
    val inner: StatSpec,
) : SeriesStatSpec<Result>

/** Wire spec for `SeriesStat.asDiscrete()`: views a series stat as a discrete stat. */
@Serializable
@SerialName("AsDiscrete")
internal data class AsDiscrete(
    /** Inner series spec being adapted to the discrete modality. */
    val inner: StatSpec,
) : DiscreteStatSpec<Result>

/** Wire spec for `SeriesStat.atX()`: feeds the `x` component of paired updates into a series stat. */
@Serializable
@SerialName("AtX")
internal data class AtX(
    /** Inner series spec receiving the `x` component. */
    val inner: StatSpec,
) : PairedStatSpec<Result>

/** Wire spec for `SeriesStat.atY()`: feeds the `y` component of paired updates into a series stat. */
@Serializable
@SerialName("AtY")
internal data class AtY(
    /** Inner series spec receiving the `y` component. */
    val inner: StatSpec,
) : PairedStatSpec<Result>

/** Wire spec for `SeriesStat.atIndex(index)`: feeds `v[index]` of vector updates into a series stat. */
@Serializable
@SerialName("AtIndex")
internal data class AtIndex(
    /** Inner series spec receiving the indexed coordinate. */
    val inner: StatSpec,
    /** Vector coordinate forwarded to [inner] on each update. */
    val index: Int,
) : VectorStatSpec<Result>

/** Wire spec for `PairedStat.atIndices(indexX, indexY)`: pairs two coordinates from vector updates. */
@Serializable
@SerialName("AtIndices")
internal data class AtIndices(
    /** Inner paired spec receiving `(v[indexX], v[indexY])`. */
    val inner: StatSpec,
    /** Vector coordinate fed as `x`. */
    val indexX: Int,
    /** Vector coordinate fed as `y`. */
    val indexY: Int,
) : VectorStatSpec<Result>

/** Wire spec for `PairedStat.withFixedX(fixedX)`: pins `x` to a constant in every paired update. */
@Serializable
@SerialName("WithFixedX")
internal data class WithFixedX(
    /** Inner paired spec whose `x` is held constant. */
    val inner: StatSpec,
    /** Constant value used for `x` on every update. */
    val fixedX: Double,
) : SeriesStatSpec<Result>

/** Wire spec for `PairedStat.withFixedY(fixedY)`: pins `y` to a constant in every paired update. */
@Serializable
@SerialName("WithFixedY")
internal data class WithFixedY(
    /** Inner paired spec whose `y` is held constant. */
    val inner: StatSpec,
    /** Constant value used for `y` on every update. */
    val fixedY: Double,
) : SeriesStatSpec<Result>

/** Wire spec for `PairedStat.withTimeAsX()`: feeds the update timestamp as `x`. */
@Serializable
@SerialName("WithTimeAsX")
internal data class WithTimeAsX(
    /** Inner paired spec whose `x` is the wall-clock timestamp (nanoseconds). */
    val inner: StatSpec,
) : SeriesStatSpec<Result>

/** Wire spec for `PairedStat.withTimeAsY()`: feeds the update timestamp as `y`. */
@Serializable
@SerialName("WithTimeAsY")
internal data class WithTimeAsY(
    /** Inner paired spec whose `y` is the wall-clock timestamp (nanoseconds). */
    val inner: StatSpec,
) : SeriesStatSpec<Result>

/** Wire spec for `SeriesStat.windowed(durationMillis, slices)`: sliding time window with [slices] buckets. */
@Serializable
@SerialName("WindowedSeries")
internal data class WindowedSeries(
    /** Inner spec replicated across the window slices. */
    val inner: StatSpec,
    /** Total window span in milliseconds. */
    val durationMillis: Long,
    /** Number of equal-length buckets inside the window. */
    val slices: Int = DEFAULT_WINDOW_SLICES,
) : SeriesStatSpec<Result>

/** Wire spec for `PairedStat.windowed(durationMillis, slices)`: sliding time window with [slices] buckets. */
@Serializable
@SerialName("WindowedPaired")
internal data class WindowedPaired(
    /** Inner spec replicated across the window slices. */
    val inner: StatSpec,
    /** Total window span in milliseconds. */
    val durationMillis: Long,
    /** Number of equal-length buckets inside the window. */
    val slices: Int = DEFAULT_WINDOW_SLICES,
) : PairedStatSpec<Result>

/** Wire spec for `VectorStat.windowed(durationMillis, slices)`: sliding time window with [slices] buckets. */
@Serializable
@SerialName("WindowedVector")
internal data class WindowedVector(
    /** Inner spec replicated across the window slices. */
    val inner: StatSpec,
    /** Total window span in milliseconds. */
    val durationMillis: Long,
    /** Number of equal-length buckets inside the window. */
    val slices: Int = DEFAULT_WINDOW_SLICES,
) : VectorStatSpec<Result>

/** Wire spec for `DiscreteStat.windowed(durationMillis, slices)`: sliding time window with [slices] buckets. */
@Serializable
@SerialName("WindowedDiscrete")
internal data class WindowedDiscrete(
    /** Inner spec replicated across the window slices. */
    val inner: StatSpec,
    /** Total window span in milliseconds. */
    val durationMillis: Long,
    /** Number of equal-length buckets inside the window. */
    val slices: Int = DEFAULT_WINDOW_SLICES,
) : DiscreteStatSpec<Result>

/** Wire spec for `SeriesStat.vectorized(dimensions)`: replicates a series stat across [dimensions] coordinates. */
@Serializable
@SerialName("Vectorized")
internal data class Vectorized(
    /** Number of vector dimensions; one [template] instance is materialised per dimension. */
    val dimensions: Int,
    /** Series spec replicated independently across every dimension. */
    val template: StatSpec,
    /**
     * When `true`, fan out only the stored entries of sparse inputs - turning per-update
     * cost into `O(nnz)` and treating absent indices as "no observation" rather than
     * "observed 0.0". Safe for additive series stats; leave `false` for Mean/Variance.
     */
    val skipZeros: Boolean = false,
) : VectorStatSpec<ResultList<Result>>

/**
 * Apply [expr] as the value transform on every update - wire-friendly
 * counterpart of `SeriesStat<R>.transformValue { ... }`. The Kotlin lambda is
 * built at materialize time and forwards every input through `expr.eval`;
 * the AST itself ([ScalarExpr]) is what travels on the wire.
 */
@Serializable
@SerialName("TransformValueSeries")
internal data class TransformValueSeries(
    /** Inner spec receiving the transformed value. */
    val inner: StatSpec,
    /** Per-update transform applied before the inner stat sees the input. */
    val expr: ScalarExpr,
) : SeriesStatSpec<Result>

/** Wire spec for `DiscreteStat.transform(expr)`: applies [expr] to every update before the inner stat sees it. */
@Serializable
@SerialName("TransformValueDiscrete")
internal data class TransformValueDiscrete(
    /** Inner discrete spec receiving the transformed value. */
    val inner: StatSpec,
    /** Per-update transform applied before the inner stat sees the input. */
    val expr: ScalarExpr,
) : DiscreteStatSpec<Result>

/** Wire spec for `SeriesStat.filter(pred)`: forwards an update only when [pred] evaluates true. */
@Serializable
@SerialName("FilterValueSeries")
internal data class FilterValueSeries(
    /** Inner spec receiving only the updates that pass [pred]. */
    val inner: StatSpec,
    /** Boolean predicate evaluated on each update; false suppresses the update. */
    val pred: BoolExpr,
) : SeriesStatSpec<Result>

/** Wire spec for `DiscreteStat.filter(pred)`: forwards an update only when [pred] evaluates true. */
@Serializable
@SerialName("FilterValueDiscrete")
internal data class FilterValueDiscrete(
    /** Inner discrete spec receiving only the updates that pass [pred]. */
    val inner: StatSpec,
    /** Boolean predicate evaluated on each update; false suppresses the update. */
    val pred: BoolExpr,
) : DiscreteStatSpec<Result>

/**
 * Apply [xExpr] / [yExpr] to map each `(x, y)` pair before update - wire-friendly
 * counterpart of `PairedStat<R>.transformPair { x, y -> ... }`. Each expr can
 * reference both [X] and [Y] of the original input.
 */
@Serializable
@SerialName("TransformPair")
internal data class TransformPair(
    /** Inner paired spec receiving the transformed pair. */
    val inner: StatSpec,
    /** Expression producing the new `x` from the original `(x, y)`. */
    val xExpr: ScalarExpr,
    /** Expression producing the new `y` from the original `(x, y)`. */
    val yExpr: ScalarExpr,
) : PairedStatSpec<Result>

/** Wire spec for `PairedStat.filter(pred)`: forwards an `(x, y)` pair only when [pred] evaluates true. */
@Serializable
@SerialName("FilterPaired")
internal data class FilterPaired(
    /** Inner paired spec receiving only the pairs that pass [pred]. */
    val inner: StatSpec,
    /** Boolean predicate evaluated on each `(x, y)` pair; false suppresses the update. */
    val pred: BoolExpr,
) : PairedStatSpec<Result>

/**
 * Apply [expr] element-wise to every entry of the incoming vector before
 * update. The expression sees the current element as [X] and can reference
 * any other element via [V]`(j)` - sufficient for normalization,
 * standardization, masking, etc. For arbitrary cross-element vector->vector
 * transforms beyond per-element evaluation, use the live `transformVector`
 * with a Kotlin lambda.
 */
@Serializable
@SerialName("TransformVectorElement")
internal data class TransformVectorElement(
    /** Inner vector spec receiving the per-element-transformed vector. */
    val inner: StatSpec,
    /** Per-element transform; sees the current element as `X` and the full vector as `V`. */
    val expr: ScalarExpr,
) : VectorStatSpec<Result>

/** Wire spec for `VectorStat.filter(pred)`: forwards an incoming vector only when [pred] evaluates true. */
@Serializable
@SerialName("FilterVector")
internal data class FilterVector(
    /** Inner vector spec receiving only the vectors that pass [pred]. */
    val inner: StatSpec,
    /** Boolean predicate evaluated on each vector; false suppresses the update. */
    val pred: BoolExpr,
) : VectorStatSpec<Result>

/**
 * Lift a [SeriesStatSpec] to a [PairedStatSpec] by reducing each `(x, y)`
 * pair to a scalar via [expr] before driving the inner stat. The expression
 * is free to reference both [X] and [Y].
 */
@Serializable
@SerialName("FoldPaired")
internal data class FoldPaired(
    /** Inner series spec receiving the folded scalar. */
    val inner: StatSpec,
    /** Expression reducing each `(x, y)` pair to a single scalar. */
    val expr: ScalarExpr,
) : PairedStatSpec<Result>

/**
 * Lift a [SeriesStatSpec] to a [VectorStatSpec] by reducing each vector to
 * a scalar via [expr] before driving the inner stat. The expression typically
 * uses [VFold] / [VDot] / [V] to consume the vector.
 */
@Serializable
@SerialName("FoldVector")
internal data class FoldVector(
    /** Inner series spec receiving the folded scalar. */
    val inner: StatSpec,
    /** Expression reducing each vector to a single scalar (typically via [VFold] / [VDot] / [V]). */
    val expr: ScalarExpr,
) : VectorStatSpec<Result>

/**
 * Lift a [PairedStatSpec] to a [VectorStatSpec] by reducing each incoming vector to a
 * pair `(xExpr, yExpr)` of scalars before driving the inner paired stat. Both
 * expressions typically use [VFold] / [VDot] / [V] to consume the vector; e.g.
 * `OLS().foldVector(xExpr = V(0), yExpr = V(1))` correlates the first two coordinates.
 */
@Serializable
@SerialName("FoldVectorPaired")
internal data class FoldVectorPaired(
    /** Inner paired spec receiving the folded `(x, y)` pair. */
    val inner: StatSpec,
    /** Expression reducing each vector to the inner stat's `x` argument. */
    val xExpr: ScalarExpr,
    /** Expression reducing each vector to the inner stat's `y` argument. */
    val yExpr: ScalarExpr,
) : VectorStatSpec<Result>

/**
 * Apply a [VectorExpr] to remap each incoming vector before update - wire
 * counterpart of `VectorStat<R>.transformVector { ... }`. Output length and
 * input length need not match; the inner stat must be parameterised for the
 * output dim.
 */
@Serializable
@SerialName("TransformVector")
internal data class TransformVector(
    /** Inner vector spec receiving the remapped vector. */
    val inner: StatSpec,
    /** Vector-to-vector expression applied per update; output length may differ from input. */
    val expr: VectorExpr,
) : VectorStatSpec<Result>

/** Wire spec for `SeriesStat.weightBy(expr)`: multiplies each update's weight by `expr.eval(value)`. */
@Serializable
@SerialName("WeightByValueSeries")
internal data class WeightByValueSeries(
    /** Inner series spec receiving the reweighted update. */
    val inner: StatSpec,
    /** Expression producing the per-update weight multiplier from the input value. */
    val expr: ScalarExpr,
) : SeriesStatSpec<Result>

/** Wire spec for `PairedStat.weightBy(expr)`: multiplies each update's weight by `expr.eval(x, y)`. */
@Serializable
@SerialName("WeightByValuePaired")
internal data class WeightByValuePaired(
    /** Inner paired spec receiving the reweighted update. */
    val inner: StatSpec,
    /** Expression producing the per-update weight multiplier from `(x, y)`. */
    val expr: ScalarExpr,
) : PairedStatSpec<Result>

/** Wire spec for `VectorStat.weightBy(expr)`: multiplies each update's weight by `expr.eval(0, 0, v)`. */
@Serializable
@SerialName("WeightByValueVector")
internal data class WeightByValueVector(
    /** Inner vector spec receiving the reweighted update. */
    val inner: StatSpec,
    /** Expression producing the per-update weight multiplier from the vector (bound as `V`). */
    val expr: ScalarExpr,
) : VectorStatSpec<Result>

/** Wire spec for `DiscreteStat.weightBy(expr)`: multiplies each update's weight by `expr.eval(value.toDouble())`. */
@Serializable
@SerialName("WeightByValueDiscrete")
internal data class WeightByValueDiscrete(
    /** Inner discrete spec receiving the reweighted update. */
    val inner: StatSpec,
    /** Expression producing the per-update weight multiplier from `value.toDouble()`. */
    val expr: ScalarExpr,
) : DiscreteStatSpec<Result>

/** Wire spec for `SeriesStat.throttle(every)`: forwards only every [every]th update. */
@Serializable
@SerialName("ThrottleSeries")
internal data class ThrottleSeries(
    /** Inner series spec receiving the throttled updates. */
    val inner: StatSpec,
    /** Stride: pass one update for every [every] arrivals. */
    val every: Int,
) : SeriesStatSpec<Result>

/** Wire spec for `PairedStat.throttle(every)`: forwards only every [every]th update. */
@Serializable
@SerialName("ThrottlePaired")
internal data class ThrottlePaired(
    /** Inner paired spec receiving the throttled updates. */
    val inner: StatSpec,
    /** Stride: pass one update for every [every] arrivals. */
    val every: Int,
) : PairedStatSpec<Result>

/** Wire spec for `VectorStat.throttle(every)`: forwards only every [every]th update. */
@Serializable
@SerialName("ThrottleVector")
internal data class ThrottleVector(
    /** Inner vector spec receiving the throttled updates. */
    val inner: StatSpec,
    /** Stride: pass one update for every [every] arrivals. */
    val every: Int,
) : VectorStatSpec<Result>

/** Wire spec for `DiscreteStat.throttle(every)`: forwards only every [every]th update. */
@Serializable
@SerialName("ThrottleDiscrete")
internal data class ThrottleDiscrete(
    /** Inner discrete spec receiving the throttled updates. */
    val inner: StatSpec,
    /** Stride: pass one update for every [every] arrivals. */
    val every: Int,
) : DiscreteStatSpec<Result>

/**
 * Wire spec for `SeriesStat.lag(k)`: forwards the value seen `k` updates ago. The
 * first `k` updates warm the internal ring and forward nothing.
 */
@Serializable
@SerialName("LagSeries")
internal data class LagSeries(
    /** Inner series spec receiving the lagged value. */
    val inner: StatSpec,
    /** Lag depth in updates; must be >= 1. */
    val k: Int,
) : SeriesStatSpec<Result>

/**
 * Wire spec for `SeriesStat.diff(k)`: forwards the k-th difference `value - value[t - k]`.
 * The first `k` updates warm the internal ring and forward nothing.
 */
@Serializable
@SerialName("DiffSeries")
internal data class DiffSeries(
    /** Inner series spec receiving the k-th difference. */
    val inner: StatSpec,
    /** Lag depth used to form the difference; must be >= 1. */
    val k: Int = 1,
) : SeriesStatSpec<Result>

/**
 * Wire spec for `SeriesStat.derivative()`: forwards `(value - prev) / (timestamp - prevTimestamp)`
 * in units-per-second. The first update warms the cell and forwards nothing; coincident timestamps
 * are dropped.
 */
@Serializable
@SerialName("DerivativeSeries")
internal data class DerivativeSeries(
    /** Inner series spec receiving the time-derivative. */
    val inner: StatSpec,
) : SeriesStatSpec<Result>

/**
 * Wire spec for `SeriesStat.hysteresis(low, high)`: maps a noisy numeric stream into a debounced
 * `0.0` / `1.0` signal using two thresholds.
 */
@Serializable
@SerialName("HysteresisSeries")
internal data class HysteresisSeries(
    /** Inner series spec receiving the debounced 0.0/1.0 stream. */
    val inner: StatSpec,
    /** Lower threshold; transitions to low state when input falls below this. */
    val low: Double,
    /** Upper threshold; transitions to high state when input rises above this. Must be >= [low]. */
    val high: Double,
) : SeriesStatSpec<Result>

/**
 * Wire spec for `SeriesStat.resampleByTime(bucket, aggregator)`: aligns the input
 * stream onto fixed wall-clock buckets and forwards one observation per closed bucket
 * to the inner stat.
 */
@Serializable
@SerialName("ResampleByTimeSeries")
internal data class ResampleByTimeSeries(
    /** Inner series spec receiving one update per closed bucket. */
    val inner: StatSpec,
    /** Bucket length in milliseconds; must be positive. */
    val bucketMillis: Long,
    /** Per-bucket reduction; defaults to [ResampleAggregator.Mean]. */
    val aggregator: ResampleAggregator = ResampleAggregator.Mean,
) : SeriesStatSpec<Result>

/**
 * Wire spec for `SeriesStat.band(k)`: derives `center ± k * scale` from any series stat whose
 * result implements [com.eignex.kumulant.core.HasCenterScale].
 */
@Serializable
@SerialName("BandSeries")
internal data class BandSeries(
    /** Inner series spec; its result must implement [com.eignex.kumulant.core.HasCenterScale]. */
    val inner: StatSpec,
    /** Scale multiplier. */
    val k: Double,
) : SeriesStatSpec<BandResult>

/**
 * Wire spec for `PairedStat.withSelfLag(k)`: lifts a paired stat into a series stat by
 * self-pairing each input with the value seen `k` updates ago. The inner paired stat
 * receives `(current, lag-k)` so a covariance / correlation stat naturally yields the
 * lag-k autocovariance / autocorrelation.
 */
@Serializable
@SerialName("WithSelfLagSeries")
internal data class WithSelfLagSeries(
    /** Inner paired spec receiving `(current, lag-k)` pairs. */
    val inner: StatSpec,
    /** Lag between paired observations; must be at least 1. */
    val k: Int,
) : SeriesStatSpec<Result>

/**
 * Wire spec for `SeriesStat.withFeedback(primary, project)`: couples an inner series stat with
 * a state-tracking primary so the projection [ScalarExpr] sees the primary's just-updated
 * snapshot via [com.eignex.kumulant.schema.expr.Center] / [com.eignex.kumulant.schema.expr.Scale]
 * (and future primary-aware AST nodes). The wrapper's result is the inner stat's snapshot.
 */
@Serializable
@SerialName("WithFeedbackSeries")
internal data class WithFeedbackSeries(
    /** Inner series spec receiving the projected value. */
    val inner: StatSpec,
    /** Primary series spec maintaining running state. */
    val primary: StatSpec,
    /** Scalar projection evaluated against `(value, primary.snapshot)`. */
    val project: ScalarExpr,
) : SeriesStatSpec<Result>

/**
 * Wire spec for `VectorStat.standardScaleFeatures(dimensions)`: element-wise z-score
 * over a [dimensions]-dimensional vector input using a per-coordinate [Variance]
 * primary fan-out.
 */
@Serializable
@SerialName("StandardScalerVector")
internal data class StandardScalerVector(
    /** Inner vector spec receiving the per-coordinate z-scored vector. */
    val inner: StatSpec,
    /** Number of vector coordinates; must match the primary fan-out size. */
    val dimensions: Int,
) : VectorStatSpec<Result>

/**
 * Wire spec for `VectorStat.minMaxScaleFeatures(dimensions, targetLow, targetHigh)`:
 * element-wise min-max scaling over a [dimensions]-dimensional vector input using a
 * per-coordinate [Range] primary fan-out.
 */
@Serializable
@SerialName("MinMaxScalerVector")
internal data class MinMaxScalerVector(
    /** Inner vector spec receiving the rescaled vector. */
    val inner: StatSpec,
    /** Number of vector coordinates. */
    val dimensions: Int,
    /** Lower bound of each coordinate's output range. */
    val targetLow: Double = DEFAULT_TARGET_LOW,
    /** Upper bound of each coordinate's output range. */
    val targetHigh: Double = DEFAULT_TARGET_HIGH,
) : VectorStatSpec<Result>

/**
 * Wire spec for `RegressionStat.standardScaleFeatures()`: element-wise z-score over
 * the inner regressor's feature vector via a per-coordinate [Variance] primary.
 * The featureSize is pulled from the inner regressor's contract.
 */
@Serializable
@SerialName("StandardScalerRegression")
internal data class StandardScalerRegression(
    /** Inner regression spec receiving the per-feature z-scored vector. */
    val inner: StatSpec,
) : RegressionStatSpec<Result>

/**
 * Wire spec for `RegressionStat.minMaxScaleFeatures(targetLow, targetHigh)`: element-wise
 * min-max scaling over the inner regressor's feature vector via a per-coordinate [Range]
 * primary.
 */
@Serializable
@SerialName("MinMaxScalerRegression")
internal data class MinMaxScalerRegression(
    /** Inner regression spec receiving the rescaled feature vector. */
    val inner: StatSpec,
    /** Lower bound of each coordinate's output range. */
    val targetLow: Double = DEFAULT_TARGET_LOW,
    /** Upper bound of each coordinate's output range. */
    val targetHigh: Double = DEFAULT_TARGET_HIGH,
) : RegressionStatSpec<Result>

/**
 * Wire spec for `PairedStat.standardScaler()`: z-scores both axes against per-axis
 * [Variance] primaries.
 */
@Serializable
@SerialName("StandardScalerPaired")
internal data class StandardScalerPaired(
    /** Inner paired spec receiving the per-axis z-scored pair. */
    val inner: StatSpec,
) : PairedStatSpec<Result>

/**
 * Wire spec for `PairedStat.minMaxScaler(targetLow, targetHigh)`: min-max scales each
 * axis against its own [Range] primary.
 */
@Serializable
@SerialName("MinMaxScalerPaired")
internal data class MinMaxScalerPaired(
    /** Inner paired spec receiving the rescaled pair. */
    val inner: StatSpec,
    /** Lower bound of each axis's output range. */
    val targetLow: Double = DEFAULT_TARGET_LOW,
    /** Upper bound of each axis's output range. */
    val targetHigh: Double = DEFAULT_TARGET_HIGH,
) : PairedStatSpec<Result>

/**
 * Wire spec for `SeriesStat.standardScaler()`: z-scores the input against a hidden
 * [Variance] primary, then forwards the standardized value to [inner]. Compact wire
 * alias for the equivalent [WithFeedbackSeries] composition.
 */
@Serializable
@SerialName("StandardScaler")
internal data class StandardScalerSeries(
    /** Inner spec receiving the z-scored value. */
    val inner: StatSpec,
) : SeriesStatSpec<Result>

/**
 * Wire spec for `SeriesStat.minMaxScaler(targetLow, targetHigh)`: min-max scales the
 * input against a hidden [Range] primary into `[targetLow, targetHigh]`, then forwards
 * the mapped value to [inner]. Compact wire alias for the equivalent
 * [WithFeedbackSeries] composition.
 */
@Serializable
@SerialName("MinMaxScaler")
internal data class MinMaxScalerSeries(
    /** Inner spec receiving the rescaled value. */
    val inner: StatSpec,
    /** Lower bound of the output range. */
    val targetLow: Double = DEFAULT_TARGET_LOW,
    /** Upper bound of the output range. */
    val targetHigh: Double = DEFAULT_TARGET_HIGH,
) : SeriesStatSpec<Result>

/** Wire spec for `SeriesStat.sample(rate, random)`: forwards each update with probability [rate]. */
@Serializable
@SerialName("SampleSeries")
internal data class SampleSeries(
    /** Inner series spec receiving the sampled updates. */
    val inner: StatSpec,
    /** Per-update keep probability in `[0.0, 1.0]`. */
    val rate: Double,
    /** Seed for the materialised PRNG so replays are deterministic. */
    val seed: Long,
) : SeriesStatSpec<Result>

/** Wire spec for `PairedStat.sample(rate, random)`: forwards each update with probability [rate]. */
@Serializable
@SerialName("SamplePaired")
internal data class SamplePaired(
    /** Inner paired spec receiving the sampled updates. */
    val inner: StatSpec,
    /** Per-update keep probability in `[0.0, 1.0]`. */
    val rate: Double,
    /** Seed for the materialised PRNG so replays are deterministic. */
    val seed: Long,
) : PairedStatSpec<Result>

/** Wire spec for `VectorStat.sample(rate, random)`: forwards each update with probability [rate]. */
@Serializable
@SerialName("SampleVector")
internal data class SampleVector(
    /** Inner vector spec receiving the sampled updates. */
    val inner: StatSpec,
    /** Per-update keep probability in `[0.0, 1.0]`. */
    val rate: Double,
    /** Seed for the materialised PRNG so replays are deterministic. */
    val seed: Long,
) : VectorStatSpec<Result>

/** Wire spec for `DiscreteStat.sample(rate, random)`: forwards each update with probability [rate]. */
@Serializable
@SerialName("SampleDiscrete")
internal data class SampleDiscrete(
    /** Inner discrete spec receiving the sampled updates. */
    val inner: StatSpec,
    /** Per-update keep probability in `[0.0, 1.0]`. */
    val rate: Double,
    /** Seed for the materialised PRNG so replays are deterministic. */
    val seed: Long,
) : DiscreteStatSpec<Result>

/** Wire spec for `RegressionStat.filter(pred)`. ScalarExpr bindings for regression:
 *  `X` is unused (0.0), `Y` is the target y, `V` is the feature vector x. */
@Serializable
@SerialName("FilterRegression")
internal data class FilterRegression(
    /** Inner regression spec receiving only the updates that pass [pred]. */
    val inner: StatSpec,
    /** Boolean predicate over `(x = V, y = Y)`; false suppresses the update. */
    val pred: BoolExpr,
) : RegressionStatSpec<Result>

/** Wire spec for `RegressionStat.transformY(expr)`. */
@Serializable
@SerialName("TransformYRegression")
internal data class TransformYRegression(
    /** Inner regression spec receiving the transformed y. */
    val inner: StatSpec,
    /** Expression remapping y; sees `Y` and `V`. */
    val expr: ScalarExpr,
) : RegressionStatSpec<Result>

/** Wire spec for `RegressionStat.transformX(expr)`. */
@Serializable
@SerialName("TransformXRegression")
internal data class TransformXRegression(
    /** Inner regression spec receiving the transformed x. */
    val inner: StatSpec,
    /** Expression producing the new vector; sees `Y` and `V`. */
    val expr: VectorExpr,
) : RegressionStatSpec<Result>

/** Wire spec for `RegressionStat.withWeight(weight)`: replaces every update's weight. */
@Serializable
@SerialName("WithWeightRegression")
internal data class WithWeightRegression(
    /** Inner regression spec whose updates are reweighted to [weight]. */
    val inner: StatSpec,
    /** Constant weight applied to every update. */
    val weight: Double,
) : RegressionStatSpec<Result>

/** Wire spec for `RegressionStat.weightBy(expr)`: multiplies each update's weight by `expr.eval(0, y, v)`. */
@Serializable
@SerialName("WeightByValueRegression")
internal data class WeightByValueRegression(
    /** Inner regression spec receiving the reweighted update. */
    val inner: StatSpec,
    /** Per-update weight multiplier from `(y = Y, x = V)`. */
    val expr: ScalarExpr,
) : RegressionStatSpec<Result>

/** Wire spec for `RegressionStat.throttle(every)`. */
@Serializable
@SerialName("ThrottleRegression")
internal data class ThrottleRegression(
    /** Inner regression spec receiving the throttled updates. */
    val inner: StatSpec,
    /** Stride; pass one update for every [every] arrivals. */
    val every: Int,
) : RegressionStatSpec<Result>

/** Wire spec for `RegressionStat.sample(rate, random)`. */
@Serializable
@SerialName("SampleRegression")
internal data class SampleRegression(
    /** Inner regression spec receiving the sampled updates. */
    val inner: StatSpec,
    /** Bernoulli keep probability in `[0.0, 1.0]`. */
    val rate: Double,
    /** Seed for the materialised PRNG. */
    val seed: Long,
) : RegressionStatSpec<Result>

/** Wire spec for `SeriesStat.foldRegression(featureSize, project)`: lift a series spec
 *  into the regression modality by projecting `(x, y)` to a scalar via [project]. */
@Serializable
@SerialName("FoldRegression")
internal data class FoldRegression(
    /** Inner series spec receiving the projected scalar. */
    val inner: StatSpec,
    /** Expected x-vector dimension; enforced at update time. */
    val featureSize: Int,
    /** Expression projecting `(x = V, y = Y)` to a scalar. */
    val project: ScalarExpr,
) : RegressionStatSpec<Result>
