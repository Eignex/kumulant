@file:Suppress("UNCHECKED_CAST")

package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.operation.BandResult
import com.eignex.kumulant.operation.ResampleAggregator
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
data class WithWeightSeries(
    /** Inner spec whose updates are weighted. */
    val inner: StatSpec,
    /** Per-update weight multiplier. */
    val weight: Double,
) : SeriesStatSpec<Result>

/** Wire spec for `PairedStat.withWeight(weight)`: multiplies every update by [weight]. */
@Serializable
@SerialName("WithWeightPaired")
data class WithWeightPaired(
    /** Inner spec whose updates are weighted. */
    val inner: StatSpec,
    /** Per-update weight multiplier. */
    val weight: Double,
) : PairedStatSpec<Result>

/** Wire spec for `VectorStat.withWeight(weight)`: multiplies every update by [weight]. */
@Serializable
@SerialName("WithWeightVector")
data class WithWeightVector(
    /** Inner spec whose updates are weighted. */
    val inner: StatSpec,
    /** Per-update weight multiplier. */
    val weight: Double,
) : VectorStatSpec<Result>

/** Wire spec for `DiscreteStat.withWeight(weight)`: multiplies every update by [weight]. */
@Serializable
@SerialName("WithWeightDiscrete")
data class WithWeightDiscrete(
    /** Inner spec whose updates are weighted. */
    val inner: StatSpec,
    /** Per-update weight multiplier. */
    val weight: Double,
) : DiscreteStatSpec<Result>

/** Wrap this series spec so every update applies the per-observation [weight] multiplier. */
fun <R : Result> SeriesStatSpec<R>.withWeight(weight: Double): SeriesStatSpec<R> =
    WithWeightSeries(this, weight) as SeriesStatSpec<R>

/** Wrap this paired spec so every update applies the per-observation [weight] multiplier. */
fun <R : Result> PairedStatSpec<R>.withWeight(weight: Double): PairedStatSpec<R> =
    WithWeightPaired(this, weight) as PairedStatSpec<R>

/** Wrap this vector spec so every update applies the per-observation [weight] multiplier. */
fun <R : Result> VectorStatSpec<R>.withWeight(weight: Double): VectorStatSpec<R> =
    WithWeightVector(this, weight) as VectorStatSpec<R>

/** Wrap this discrete spec so every update applies the per-observation [weight] multiplier. */
fun <R : Result> DiscreteStatSpec<R>.withWeight(weight: Double): DiscreteStatSpec<R> =
    WithWeightDiscrete(this, weight) as DiscreteStatSpec<R>

/** Wire spec for `SeriesStat.withValue(value)`: pins every update to [value]. */
@Serializable
@SerialName("WithValueSeries")
data class WithValueSeries(
    /** Inner spec whose updates use the fixed [value]. */
    val inner: StatSpec,
    /** Value pushed into the inner stat on every update. */
    val value: Double,
) : SeriesStatSpec<Result>

/** Wire spec for `DiscreteStat.withValue(value)`: pins every update to [value]. */
@Serializable
@SerialName("WithValueDiscrete")
data class WithValueDiscrete(
    /** Inner spec whose updates use the fixed [value]. */
    val inner: StatSpec,
    /** Value pushed into the inner stat on every update. */
    val value: Long,
) : DiscreteStatSpec<Result>

/** Wrap this series spec so every update pushes the constant [value] regardless of input. */
fun <R : Result> SeriesStatSpec<R>.withValue(value: Double): SeriesStatSpec<R> =
    WithValueSeries(this, value) as SeriesStatSpec<R>

/** Wrap this discrete spec so every update pushes the constant [value] regardless of input. */
fun <R : Result> DiscreteStatSpec<R>.withValue(value: Long): DiscreteStatSpec<R> =
    WithValueDiscrete(this, value) as DiscreteStatSpec<R>

/** Wire spec for `DiscreteStat.asSeries()`: views a discrete stat as a series stat. */
@Serializable
@SerialName("AsSeries")
data class AsSeries(
    /** Inner discrete spec being adapted to the series modality. */
    val inner: StatSpec,
) : SeriesStatSpec<Result>

/** Wire spec for `SeriesStat.asDiscrete()`: views a series stat as a discrete stat. */
@Serializable
@SerialName("AsDiscrete")
data class AsDiscrete(
    /** Inner series spec being adapted to the discrete modality. */
    val inner: StatSpec,
) : DiscreteStatSpec<Result>

/** Adapt a discrete spec into a series spec - the series sees `value.toDouble()` per update. */
fun <R : Result> DiscreteStatSpec<R>.asSeries(): SeriesStatSpec<R> = AsSeries(this) as SeriesStatSpec<R>

/** Adapt a series spec into a discrete spec - the discrete sees `value.toLong()` per update. */
fun <R : Result> SeriesStatSpec<R>.asDiscrete(): DiscreteStatSpec<R> = AsDiscrete(this) as DiscreteStatSpec<R>

/** Wire spec for `SeriesStat.atX()`: feeds the `x` component of paired updates into a series stat. */
@Serializable
@SerialName("AtX")
data class AtX(
    /** Inner series spec receiving the `x` component. */
    val inner: StatSpec,
) : PairedStatSpec<Result>

/** Wire spec for `SeriesStat.atY()`: feeds the `y` component of paired updates into a series stat. */
@Serializable
@SerialName("AtY")
data class AtY(
    /** Inner series spec receiving the `y` component. */
    val inner: StatSpec,
) : PairedStatSpec<Result>

/** Wire spec for `SeriesStat.atIndex(index)`: feeds `v[index]` of vector updates into a series stat. */
@Serializable
@SerialName("AtIndex")
data class AtIndex(
    /** Inner series spec receiving the indexed coordinate. */
    val inner: StatSpec,
    /** Vector coordinate forwarded to [inner] on each update. */
    val index: Int,
) : VectorStatSpec<Result>

/** Wire spec for `PairedStat.atIndices(indexX, indexY)`: pairs two coordinates from vector updates. */
@Serializable
@SerialName("AtIndices")
data class AtIndices(
    /** Inner paired spec receiving `(v[indexX], v[indexY])`. */
    val inner: StatSpec,
    /** Vector coordinate fed as `x`. */
    val indexX: Int,
    /** Vector coordinate fed as `y`. */
    val indexY: Int,
) : VectorStatSpec<Result>

/** Adapt a series spec into a paired spec by consuming the `x` component of each pair. */
fun <R : Result> SeriesStatSpec<R>.atX(): PairedStatSpec<R> = AtX(this) as PairedStatSpec<R>

/** Adapt a series spec into a paired spec by consuming the `y` component of each pair. */
fun <R : Result> SeriesStatSpec<R>.atY(): PairedStatSpec<R> = AtY(this) as PairedStatSpec<R>

/** Adapt a series spec into a vector spec by consuming the [index]-th coordinate of each vector. */
fun <R : Result> SeriesStatSpec<R>.atIndex(index: Int): VectorStatSpec<R> = AtIndex(this, index) as VectorStatSpec<R>

/** Adapt a paired spec into a vector spec by consuming the [indexX] / [indexY] coordinates. */
fun <R : Result> PairedStatSpec<R>.atIndices(indexX: Int, indexY: Int): VectorStatSpec<R> =
    AtIndices(this, indexX, indexY) as VectorStatSpec<R>

/** Wire spec for `PairedStat.withFixedX(fixedX)`: pins `x` to a constant in every paired update. */
@Serializable
@SerialName("WithFixedX")
data class WithFixedX(
    /** Inner paired spec whose `x` is held constant. */
    val inner: StatSpec,
    /** Constant value used for `x` on every update. */
    val fixedX: Double,
) : SeriesStatSpec<Result>

/** Wire spec for `PairedStat.withFixedY(fixedY)`: pins `y` to a constant in every paired update. */
@Serializable
@SerialName("WithFixedY")
data class WithFixedY(
    /** Inner paired spec whose `y` is held constant. */
    val inner: StatSpec,
    /** Constant value used for `y` on every update. */
    val fixedY: Double,
) : SeriesStatSpec<Result>

/** Wire spec for `PairedStat.withTimeAsX()`: feeds the update timestamp as `x`. */
@Serializable
@SerialName("WithTimeAsX")
data class WithTimeAsX(
    /** Inner paired spec whose `x` is the wall-clock timestamp (nanoseconds). */
    val inner: StatSpec,
) : SeriesStatSpec<Result>

/** Wire spec for `PairedStat.withTimeAsY()`: feeds the update timestamp as `y`. */
@Serializable
@SerialName("WithTimeAsY")
data class WithTimeAsY(
    /** Inner paired spec whose `y` is the wall-clock timestamp (nanoseconds). */
    val inner: StatSpec,
) : SeriesStatSpec<Result>

/** Adapt a paired spec into a series spec by pinning `x` to [fixedX]. */
fun <R : Result> PairedStatSpec<R>.withFixedX(fixedX: Double): SeriesStatSpec<R> =
    WithFixedX(this, fixedX) as SeriesStatSpec<R>

/** Adapt a paired spec into a series spec by pinning `y` to [fixedY]. */
fun <R : Result> PairedStatSpec<R>.withFixedY(fixedY: Double): SeriesStatSpec<R> =
    WithFixedY(this, fixedY) as SeriesStatSpec<R>

/** Adapt a paired spec into a series spec by using the update timestamp as `x`. */
fun <R : Result> PairedStatSpec<R>.withTimeAsX(): SeriesStatSpec<R> = WithTimeAsX(this) as SeriesStatSpec<R>

/** Adapt a paired spec into a series spec by using the update timestamp as `y`. */
fun <R : Result> PairedStatSpec<R>.withTimeAsY(): SeriesStatSpec<R> = WithTimeAsY(this) as SeriesStatSpec<R>

/** Wire spec for `SeriesStat.windowed(durationMillis, slices)`: sliding time window with [slices] buckets. */
@Serializable
@SerialName("WindowedSeries")
data class WindowedSeries(
    /** Inner spec replicated across the window slices. */
    val inner: StatSpec,
    /** Total window span in milliseconds. */
    val durationMillis: Long,
    /** Number of equal-length buckets inside the window. */
    val slices: Int = 10,
) : SeriesStatSpec<Result>

/** Wire spec for `PairedStat.windowed(durationMillis, slices)`: sliding time window with [slices] buckets. */
@Serializable
@SerialName("WindowedPaired")
data class WindowedPaired(
    /** Inner spec replicated across the window slices. */
    val inner: StatSpec,
    /** Total window span in milliseconds. */
    val durationMillis: Long,
    /** Number of equal-length buckets inside the window. */
    val slices: Int = 10,
) : PairedStatSpec<Result>

/** Wire spec for `VectorStat.windowed(durationMillis, slices)`: sliding time window with [slices] buckets. */
@Serializable
@SerialName("WindowedVector")
data class WindowedVector(
    /** Inner spec replicated across the window slices. */
    val inner: StatSpec,
    /** Total window span in milliseconds. */
    val durationMillis: Long,
    /** Number of equal-length buckets inside the window. */
    val slices: Int = 10,
) : VectorStatSpec<Result>

/** Wire spec for `DiscreteStat.windowed(durationMillis, slices)`: sliding time window with [slices] buckets. */
@Serializable
@SerialName("WindowedDiscrete")
data class WindowedDiscrete(
    /** Inner spec replicated across the window slices. */
    val inner: StatSpec,
    /** Total window span in milliseconds. */
    val durationMillis: Long,
    /** Number of equal-length buckets inside the window. */
    val slices: Int = 10,
) : DiscreteStatSpec<Result>

/** Wrap this series spec in a sliding time window of [durationMillis] split into [slices] buckets. */
fun <R : Result> SeriesStatSpec<R>.windowed(durationMillis: Long, slices: Int = 10): SeriesStatSpec<R> =
    WindowedSeries(this, durationMillis, slices) as SeriesStatSpec<R>

/** Wrap this paired spec in a sliding time window of [durationMillis] split into [slices] buckets. */
fun <R : Result> PairedStatSpec<R>.windowed(durationMillis: Long, slices: Int = 10): PairedStatSpec<R> =
    WindowedPaired(this, durationMillis, slices) as PairedStatSpec<R>

/** Wrap this vector spec in a sliding time window of [durationMillis] split into [slices] buckets. */
fun <R : Result> VectorStatSpec<R>.windowed(durationMillis: Long, slices: Int = 10): VectorStatSpec<R> =
    WindowedVector(this, durationMillis, slices) as VectorStatSpec<R>

/** Wrap this discrete spec in a sliding time window of [durationMillis] split into [slices] buckets. */
fun <R : Result> DiscreteStatSpec<R>.windowed(durationMillis: Long, slices: Int = 10): DiscreteStatSpec<R> =
    WindowedDiscrete(this, durationMillis, slices) as DiscreteStatSpec<R>

/** Wire spec for `SeriesStat.vectorized(dimensions)`: replicates a series stat across [dimensions] coordinates. */
@Serializable
@SerialName("Vectorized")
data class Vectorized(
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

/** Lift a series spec to a vector spec by replicating it across every coordinate of an N-dim input. */
fun <R : Result> SeriesStatSpec<R>.vectorized(
    dimensions: Int,
    skipZeros: Boolean = false,
): VectorStatSpec<ResultList<R>> = Vectorized(dimensions, this, skipZeros) as VectorStatSpec<ResultList<R>>

/**
 * Apply [expr] as the value transform on every update - wire-friendly
 * counterpart of `SeriesStat<R>.transformValue { ... }`. The Kotlin lambda is
 * built at materialize time and forwards every input through `expr.eval`;
 * the AST itself ([ScalarExpr]) is what travels on the wire.
 */
@Serializable
@SerialName("TransformValueSeries")
data class TransformValueSeries(
    /** Inner spec receiving the transformed value. */
    val inner: StatSpec,
    /** Per-update transform applied before the inner stat sees the input. */
    val expr: ScalarExpr,
) : SeriesStatSpec<Result>

/** Wire spec for `DiscreteStat.transform(expr)`: applies [expr] to every update before the inner stat sees it. */
@Serializable
@SerialName("TransformValueDiscrete")
data class TransformValueDiscrete(
    /** Inner discrete spec receiving the transformed value. */
    val inner: StatSpec,
    /** Per-update transform applied before the inner stat sees the input. */
    val expr: ScalarExpr,
) : DiscreteStatSpec<Result>

/** Wire spec for `SeriesStat.filter(pred)`: forwards an update only when [pred] evaluates true. */
@Serializable
@SerialName("FilterValueSeries")
data class FilterValueSeries(
    /** Inner spec receiving only the updates that pass [pred]. */
    val inner: StatSpec,
    /** Boolean predicate evaluated on each update; false suppresses the update. */
    val pred: BoolExpr,
) : SeriesStatSpec<Result>

/** Wire spec for `DiscreteStat.filter(pred)`: forwards an update only when [pred] evaluates true. */
@Serializable
@SerialName("FilterValueDiscrete")
data class FilterValueDiscrete(
    /** Inner discrete spec receiving only the updates that pass [pred]. */
    val inner: StatSpec,
    /** Boolean predicate evaluated on each update; false suppresses the update. */
    val pred: BoolExpr,
) : DiscreteStatSpec<Result>

/** Wrap this series spec to apply [expr] to every update before the inner stat sees it. */
fun <R : Result> SeriesStatSpec<R>.transform(expr: ScalarExpr): SeriesStatSpec<R> =
    TransformValueSeries(this, expr) as SeriesStatSpec<R>

/** Wrap this discrete spec to apply [expr] to every update before the inner stat sees it. */
fun <R : Result> DiscreteStatSpec<R>.transform(expr: ScalarExpr): DiscreteStatSpec<R> =
    TransformValueDiscrete(this, expr) as DiscreteStatSpec<R>

/** Wrap this series spec so updates are forwarded only when [pred] evaluates true. */
fun <R : Result> SeriesStatSpec<R>.filter(pred: BoolExpr): SeriesStatSpec<R> =
    FilterValueSeries(this, pred) as SeriesStatSpec<R>

/** Wrap this discrete spec so updates are forwarded only when [pred] evaluates true. */
fun <R : Result> DiscreteStatSpec<R>.filter(pred: BoolExpr): DiscreteStatSpec<R> =
    FilterValueDiscrete(this, pred) as DiscreteStatSpec<R>

/**
 * Apply [xExpr] / [yExpr] to map each `(x, y)` pair before update - wire-friendly
 * counterpart of `PairedStat<R>.transformPair { x, y -> ... }`. Each expr can
 * reference both [X] and [Y] of the original input.
 */
@Serializable
@SerialName("TransformPair")
data class TransformPair(
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
data class FilterPaired(
    /** Inner paired spec receiving only the pairs that pass [pred]. */
    val inner: StatSpec,
    /** Boolean predicate evaluated on each `(x, y)` pair; false suppresses the update. */
    val pred: BoolExpr,
) : PairedStatSpec<Result>

/** Wrap this paired spec so each `(x, y)` is remapped via [xExpr]/[yExpr] before the inner stat sees it. */
fun <R : Result> PairedStatSpec<R>.transformPair(xExpr: ScalarExpr, yExpr: ScalarExpr): PairedStatSpec<R> =
    TransformPair(this, xExpr, yExpr) as PairedStatSpec<R>

/** Map only the x coordinate; y stays as-is. */
fun <R : Result> PairedStatSpec<R>.transformX(expr: ScalarExpr): PairedStatSpec<R> = transformPair(expr, Y)

/** Map only the y coordinate; x stays as-is. */
fun <R : Result> PairedStatSpec<R>.transformY(expr: ScalarExpr): PairedStatSpec<R> = transformPair(X, expr)

/** Wrap this paired spec so updates are forwarded only when [pred] evaluates true on `(x, y)`. */
fun <R : Result> PairedStatSpec<R>.filter(pred: BoolExpr): PairedStatSpec<R> =
    FilterPaired(this, pred) as PairedStatSpec<R>

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
data class TransformVectorElement(
    /** Inner vector spec receiving the per-element-transformed vector. */
    val inner: StatSpec,
    /** Per-element transform; sees the current element as `X` and the full vector as `V`. */
    val expr: ScalarExpr,
) : VectorStatSpec<Result>

/** Wire spec for `VectorStat.filter(pred)`: forwards an incoming vector only when [pred] evaluates true. */
@Serializable
@SerialName("FilterVector")
data class FilterVector(
    /** Inner vector spec receiving only the vectors that pass [pred]. */
    val inner: StatSpec,
    /** Boolean predicate evaluated on each vector; false suppresses the update. */
    val pred: BoolExpr,
) : VectorStatSpec<Result>

/** Wrap this vector spec to apply [expr] to every element of each incoming vector before update. */
fun <R : Result> VectorStatSpec<R>.transformElement(expr: ScalarExpr): VectorStatSpec<R> =
    TransformVectorElement(this, expr) as VectorStatSpec<R>

/** Wrap this vector spec so updates are forwarded only when [pred] evaluates true on the full vector. */
fun <R : Result> VectorStatSpec<R>.filter(pred: BoolExpr): VectorStatSpec<R> =
    FilterVector(this, pred) as VectorStatSpec<R>

/**
 * Lift a [SeriesStatSpec] to a [PairedStatSpec] by reducing each `(x, y)`
 * pair to a scalar via [expr] before driving the inner stat. The expression
 * is free to reference both [X] and [Y].
 */
@Serializable
@SerialName("FoldPaired")
data class FoldPaired(
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
data class FoldVector(
    /** Inner series spec receiving the folded scalar. */
    val inner: StatSpec,
    /** Expression reducing each vector to a single scalar (typically via [VFold] / [VDot] / [V]). */
    val expr: ScalarExpr,
) : VectorStatSpec<Result>

/** Lift this series spec to a paired spec, reducing every `(x, y)` to a scalar via [expr]. */
fun <R : Result> SeriesStatSpec<R>.foldPaired(expr: ScalarExpr): PairedStatSpec<R> =
    FoldPaired(this, expr) as PairedStatSpec<R>

/** Lift this series spec to a vector spec, reducing every vector to a scalar via [expr]. */
fun <R : Result> SeriesStatSpec<R>.foldVector(expr: ScalarExpr): VectorStatSpec<R> =
    FoldVector(this, expr) as VectorStatSpec<R>

/**
 * Lift a [PairedStatSpec] to a [VectorStatSpec] by reducing each incoming vector to a
 * pair `(xExpr, yExpr)` of scalars before driving the inner paired stat. Both
 * expressions typically use [VFold] / [VDot] / [V] to consume the vector; e.g.
 * `OLS().foldVector(xExpr = V(0), yExpr = V(1))` correlates the first two coordinates.
 */
@Serializable
@SerialName("FoldVectorPaired")
data class FoldVectorPaired(
    /** Inner paired spec receiving the folded `(x, y)` pair. */
    val inner: StatSpec,
    /** Expression reducing each vector to the inner stat's `x` argument. */
    val xExpr: ScalarExpr,
    /** Expression reducing each vector to the inner stat's `y` argument. */
    val yExpr: ScalarExpr,
) : VectorStatSpec<Result>

/**
 * Lift this paired spec to a vector spec, reducing every vector to a pair
 * `(xExpr, yExpr)` of scalars via [xExpr] and [yExpr].
 */
fun <R : Result> PairedStatSpec<R>.foldVector(xExpr: ScalarExpr, yExpr: ScalarExpr): VectorStatSpec<R> =
    FoldVectorPaired(this, xExpr, yExpr) as VectorStatSpec<R>

/**
 * Apply a [VectorExpr] to remap each incoming vector before update - wire
 * counterpart of `VectorStat<R>.transformVector { ... }`. Output length and
 * input length need not match; the inner stat must be parameterised for the
 * output dim.
 */
@Serializable
@SerialName("TransformVector")
data class TransformVector(
    /** Inner vector spec receiving the remapped vector. */
    val inner: StatSpec,
    /** Vector-to-vector expression applied per update; output length may differ from input. */
    val expr: VectorExpr,
) : VectorStatSpec<Result>

/** Wrap this vector spec so each incoming vector is remapped through [expr] before update. */
fun <R : Result> VectorStatSpec<R>.transformVector(expr: VectorExpr): VectorStatSpec<R> =
    TransformVector(this, expr) as VectorStatSpec<R>

/** Wire spec for `SeriesStat.weightBy(expr)`: multiplies each update's weight by `expr.eval(value)`. */
@Serializable
@SerialName("WeightByValueSeries")
data class WeightByValueSeries(
    /** Inner series spec receiving the reweighted update. */
    val inner: StatSpec,
    /** Expression producing the per-update weight multiplier from the input value. */
    val expr: ScalarExpr,
) : SeriesStatSpec<Result>

/** Wire spec for `PairedStat.weightBy(expr)`: multiplies each update's weight by `expr.eval(x, y)`. */
@Serializable
@SerialName("WeightByValuePaired")
data class WeightByValuePaired(
    /** Inner paired spec receiving the reweighted update. */
    val inner: StatSpec,
    /** Expression producing the per-update weight multiplier from `(x, y)`. */
    val expr: ScalarExpr,
) : PairedStatSpec<Result>

/** Wire spec for `VectorStat.weightBy(expr)`: multiplies each update's weight by `expr.eval(0, 0, v)`. */
@Serializable
@SerialName("WeightByValueVector")
data class WeightByValueVector(
    /** Inner vector spec receiving the reweighted update. */
    val inner: StatSpec,
    /** Expression producing the per-update weight multiplier from the vector (bound as `V`). */
    val expr: ScalarExpr,
) : VectorStatSpec<Result>

/** Wire spec for `DiscreteStat.weightBy(expr)`: multiplies each update's weight by `expr.eval(value.toDouble())`. */
@Serializable
@SerialName("WeightByValueDiscrete")
data class WeightByValueDiscrete(
    /** Inner discrete spec receiving the reweighted update. */
    val inner: StatSpec,
    /** Expression producing the per-update weight multiplier from `value.toDouble()`. */
    val expr: ScalarExpr,
) : DiscreteStatSpec<Result>

/** Wrap this series spec so every update's weight is multiplied by `expr.eval(value)`. */
fun <R : Result> SeriesStatSpec<R>.weightBy(expr: ScalarExpr): SeriesStatSpec<R> =
    WeightByValueSeries(this, expr) as SeriesStatSpec<R>

/** Wrap this paired spec so every update's weight is multiplied by `expr.eval(x, y)`. */
fun <R : Result> PairedStatSpec<R>.weightBy(expr: ScalarExpr): PairedStatSpec<R> =
    WeightByValuePaired(this, expr) as PairedStatSpec<R>

/** Wrap this vector spec so every update's weight is multiplied by `expr.eval(0, 0, vec)`. */
fun <R : Result> VectorStatSpec<R>.weightBy(expr: ScalarExpr): VectorStatSpec<R> =
    WeightByValueVector(this, expr) as VectorStatSpec<R>

/** Wrap this discrete spec so every update's weight is multiplied by `expr.eval(value.toDouble())`. */
fun <R : Result> DiscreteStatSpec<R>.weightBy(expr: ScalarExpr): DiscreteStatSpec<R> =
    WeightByValueDiscrete(this, expr) as DiscreteStatSpec<R>

/** Wire spec for `SeriesStat.throttle(every)`: forwards only every [every]th update. */
@Serializable
@SerialName("ThrottleSeries")
data class ThrottleSeries(
    /** Inner series spec receiving the throttled updates. */
    val inner: StatSpec,
    /** Stride: pass one update for every [every] arrivals. */
    val every: Int,
) : SeriesStatSpec<Result>

/** Wire spec for `PairedStat.throttle(every)`: forwards only every [every]th update. */
@Serializable
@SerialName("ThrottlePaired")
data class ThrottlePaired(
    /** Inner paired spec receiving the throttled updates. */
    val inner: StatSpec,
    /** Stride: pass one update for every [every] arrivals. */
    val every: Int,
) : PairedStatSpec<Result>

/** Wire spec for `VectorStat.throttle(every)`: forwards only every [every]th update. */
@Serializable
@SerialName("ThrottleVector")
data class ThrottleVector(
    /** Inner vector spec receiving the throttled updates. */
    val inner: StatSpec,
    /** Stride: pass one update for every [every] arrivals. */
    val every: Int,
) : VectorStatSpec<Result>

/** Wire spec for `DiscreteStat.throttle(every)`: forwards only every [every]th update. */
@Serializable
@SerialName("ThrottleDiscrete")
data class ThrottleDiscrete(
    /** Inner discrete spec receiving the throttled updates. */
    val inner: StatSpec,
    /** Stride: pass one update for every [every] arrivals. */
    val every: Int,
) : DiscreteStatSpec<Result>

/** Wrap this series spec so it only sees one in every [every] updates. */
fun <R : Result> SeriesStatSpec<R>.throttle(every: Int): SeriesStatSpec<R> =
    ThrottleSeries(this, every) as SeriesStatSpec<R>

/** Wrap this paired spec so it only sees one in every [every] updates. */
fun <R : Result> PairedStatSpec<R>.throttle(every: Int): PairedStatSpec<R> =
    ThrottlePaired(this, every) as PairedStatSpec<R>

/** Wrap this vector spec so it only sees one in every [every] updates. */
fun <R : Result> VectorStatSpec<R>.throttle(every: Int): VectorStatSpec<R> =
    ThrottleVector(this, every) as VectorStatSpec<R>

/** Wrap this discrete spec so it only sees one in every [every] updates. */
fun <R : Result> DiscreteStatSpec<R>.throttle(every: Int): DiscreteStatSpec<R> =
    ThrottleDiscrete(this, every) as DiscreteStatSpec<R>

/**
 * Wire spec for `SeriesStat.lag(k)`: forwards the value seen `k` updates ago. The
 * first `k` updates warm the internal ring and forward nothing.
 */
@Serializable
@SerialName("LagSeries")
data class LagSeries(
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
data class DiffSeries(
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
data class DerivativeSeries(
    /** Inner series spec receiving the time-derivative. */
    val inner: StatSpec,
) : SeriesStatSpec<Result>

/** Wrap this series spec to forward the value seen [k] updates ago. */
fun <R : Result> SeriesStatSpec<R>.lag(k: Int): SeriesStatSpec<R> = LagSeries(this, k) as SeriesStatSpec<R>

/** Wrap this series spec to forward the k-th difference `value - value[t - k]`. */
fun <R : Result> SeriesStatSpec<R>.diff(k: Int = 1): SeriesStatSpec<R> = DiffSeries(this, k) as SeriesStatSpec<R>

/** Wrap this series spec to forward the per-second time derivative of the value stream. */
fun <R : Result> SeriesStatSpec<R>.derivative(): SeriesStatSpec<R> = DerivativeSeries(this) as SeriesStatSpec<R>

/**
 * Wire spec for `SeriesStat.hysteresis(low, high)`: maps a noisy numeric stream into a debounced
 * `0.0` / `1.0` signal using two thresholds.
 */
@Serializable
@SerialName("HysteresisSeries")
data class HysteresisSeries(
    /** Inner series spec receiving the debounced 0.0/1.0 stream. */
    val inner: StatSpec,
    /** Lower threshold; transitions to low state when input falls below this. */
    val low: Double,
    /** Upper threshold; transitions to high state when input rises above this. Must be >= [low]. */
    val high: Double,
) : SeriesStatSpec<Result>

/** Wrap this series spec to debounce its input into a 0.0/1.0 stream via two-threshold hysteresis. */
fun <R : Result> SeriesStatSpec<R>.hysteresis(low: Double, high: Double): SeriesStatSpec<R> =
    HysteresisSeries(this, low, high) as SeriesStatSpec<R>

/**
 * Wire spec for `SeriesStat.resampleByTime(bucket, aggregator)`: aligns the input
 * stream onto fixed wall-clock buckets and forwards one observation per closed bucket
 * to the inner stat.
 */
@Serializable
@SerialName("ResampleByTimeSeries")
data class ResampleByTimeSeries(
    /** Inner series spec receiving one update per closed bucket. */
    val inner: StatSpec,
    /** Bucket length in milliseconds; must be positive. */
    val bucketMillis: Long,
    /** Per-bucket reduction; defaults to [ResampleAggregator.Mean]. */
    val aggregator: ResampleAggregator = ResampleAggregator.Mean,
) : SeriesStatSpec<Result>

/** Wrap this series spec to forward one per-bucket summary using [aggregator]. */
fun <R : Result> SeriesStatSpec<R>.resampleByTime(
    bucketMillis: Long,
    aggregator: ResampleAggregator = ResampleAggregator.Mean,
): SeriesStatSpec<R> = ResampleByTimeSeries(this, bucketMillis, aggregator) as SeriesStatSpec<R>

/**
 * Wire spec for `SeriesStat.band(k)`: derives `center ± k * scale` from any series stat whose
 * result implements [com.eignex.kumulant.core.HasCenterScale].
 */
@Serializable
@SerialName("BandSeries")
data class BandSeries(
    /** Inner series spec; its result must implement [com.eignex.kumulant.core.HasCenterScale]. */
    val inner: StatSpec,
    /** Scale multiplier. */
    val k: Double,
) : SeriesStatSpec<BandResult>

/** Wrap this series spec to expose a `[lower, upper]` band of width [k] * scale around center. */
fun SeriesStatSpec<*>.band(k: Double): SeriesStatSpec<BandResult> = BandSeries(this, k)

/**
 * Wire spec for `PairedStat.withSelfLag(k)`: lifts a paired stat into a series stat by
 * self-pairing each input with the value seen `k` updates ago. The inner paired stat
 * receives `(current, lag-k)` so a covariance / correlation stat naturally yields the
 * lag-k autocovariance / autocorrelation.
 */
@Serializable
@SerialName("WithSelfLagSeries")
data class WithSelfLagSeries(
    /** Inner paired spec receiving `(current, lag-k)` pairs. */
    val inner: StatSpec,
    /** Lag between paired observations; must be at least 1. */
    val k: Int,
) : SeriesStatSpec<Result>

/** Lift a paired spec into a series spec by self-pairing each input with the value seen [k] updates ago. */
fun <R : Result> PairedStatSpec<R>.withSelfLag(k: Int): SeriesStatSpec<R> =
    WithSelfLagSeries(this, k) as SeriesStatSpec<R>

/**
 * Wire spec for `SeriesStat.withFeedback(primary, project)`: couples an inner series stat with
 * a state-tracking primary so the projection [ScalarExpr] sees the primary's just-updated
 * snapshot via [com.eignex.kumulant.schema.Center] / [com.eignex.kumulant.schema.Scale]
 * (and future primary-aware AST nodes). The wrapper's result is the inner stat's snapshot.
 */
@Serializable
@SerialName("WithFeedbackSeries")
data class WithFeedbackSeries(
    /** Inner series spec receiving the projected value. */
    val inner: StatSpec,
    /** Primary series spec maintaining running state. */
    val primary: StatSpec,
    /** Scalar projection evaluated against `(value, primary.snapshot)`. */
    val project: ScalarExpr,
) : SeriesStatSpec<Result>

/** Wrap this inner series spec with a feedback primary; the projection AST sees the primary snapshot. */
fun <R : Result> SeriesStatSpec<R>.withFeedback(primary: SeriesStatSpec<*>, project: ScalarExpr): SeriesStatSpec<R> =
    WithFeedbackSeries(this, primary, project) as SeriesStatSpec<R>

/** Wire spec for `SeriesStat.sample(rate, random)`: forwards each update with probability [rate]. */
@Serializable
@SerialName("SampleSeries")
data class SampleSeries(
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
data class SamplePaired(
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
data class SampleVector(
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
data class SampleDiscrete(
    /** Inner discrete spec receiving the sampled updates. */
    val inner: StatSpec,
    /** Per-update keep probability in `[0.0, 1.0]`. */
    val rate: Double,
    /** Seed for the materialised PRNG so replays are deterministic. */
    val seed: Long,
) : DiscreteStatSpec<Result>

/** Wrap this series spec to keep each update with probability [rate]; [seed] feeds the PRNG. */
fun <R : Result> SeriesStatSpec<R>.sample(rate: Double, seed: Long): SeriesStatSpec<R> =
    SampleSeries(this, rate, seed) as SeriesStatSpec<R>

/** Wrap this paired spec to keep each update with probability [rate]; [seed] feeds the PRNG. */
fun <R : Result> PairedStatSpec<R>.sample(rate: Double, seed: Long): PairedStatSpec<R> =
    SamplePaired(this, rate, seed) as PairedStatSpec<R>

/** Wrap this vector spec to keep each update with probability [rate]; [seed] feeds the PRNG. */
fun <R : Result> VectorStatSpec<R>.sample(rate: Double, seed: Long): VectorStatSpec<R> =
    SampleVector(this, rate, seed) as VectorStatSpec<R>

/** Wrap this discrete spec to keep each update with probability [rate]; [seed] feeds the PRNG. */
fun <R : Result> DiscreteStatSpec<R>.sample(rate: Double, seed: Long): DiscreteStatSpec<R> =
    SampleDiscrete(this, rate, seed) as DiscreteStatSpec<R>

/** Wire spec for `RegressionStat.filter(pred)`. ScalarExpr bindings for regression:
 *  `X` is unused (0.0), `Y` is the target y, `V` is the feature vector x. */
@Serializable
@SerialName("FilterRegression")
data class FilterRegression(
    /** Inner regression spec receiving only the updates that pass [pred]. */
    val inner: StatSpec,
    /** Boolean predicate over `(x = V, y = Y)`; false suppresses the update. */
    val pred: BoolExpr,
) : RegressionStatSpec<Result>

/** Wire spec for `RegressionStat.transformY(expr)`. */
@Serializable
@SerialName("TransformYRegression")
data class TransformYRegression(
    /** Inner regression spec receiving the transformed y. */
    val inner: StatSpec,
    /** Expression remapping y; sees `Y` and `V`. */
    val expr: ScalarExpr,
) : RegressionStatSpec<Result>

/** Wire spec for `RegressionStat.transformX(expr)`. */
@Serializable
@SerialName("TransformXRegression")
data class TransformXRegression(
    /** Inner regression spec receiving the transformed x. */
    val inner: StatSpec,
    /** Expression producing the new vector; sees `Y` and `V`. */
    val expr: VectorExpr,
) : RegressionStatSpec<Result>

/** Wire spec for `RegressionStat.withWeight(weight)`: replaces every update's weight. */
@Serializable
@SerialName("WithWeightRegression")
data class WithWeightRegression(
    /** Inner regression spec whose updates are reweighted to [weight]. */
    val inner: StatSpec,
    /** Constant weight applied to every update. */
    val weight: Double,
) : RegressionStatSpec<Result>

/** Wire spec for `RegressionStat.weightBy(expr)`: multiplies each update's weight by `expr.eval(0, y, v)`. */
@Serializable
@SerialName("WeightByValueRegression")
data class WeightByValueRegression(
    /** Inner regression spec receiving the reweighted update. */
    val inner: StatSpec,
    /** Per-update weight multiplier from `(y = Y, x = V)`. */
    val expr: ScalarExpr,
) : RegressionStatSpec<Result>

/** Wire spec for `RegressionStat.throttle(every)`. */
@Serializable
@SerialName("ThrottleRegression")
data class ThrottleRegression(
    /** Inner regression spec receiving the throttled updates. */
    val inner: StatSpec,
    /** Stride; pass one update for every [every] arrivals. */
    val every: Int,
) : RegressionStatSpec<Result>

/** Wire spec for `RegressionStat.sample(rate, random)`. */
@Serializable
@SerialName("SampleRegression")
data class SampleRegression(
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
data class FoldRegression(
    /** Inner series spec receiving the projected scalar. */
    val inner: StatSpec,
    /** Expected x-vector dimension; enforced at update time. */
    val featureSize: Int,
    /** Expression projecting `(x = V, y = Y)` to a scalar. */
    val project: ScalarExpr,
) : RegressionStatSpec<Result>

/** Wrap this regression spec so updates are forwarded only when [pred] evaluates true. */
fun <R : Result> RegressionStatSpec<R>.filter(pred: BoolExpr): RegressionStatSpec<R> =
    FilterRegression(this, pred) as RegressionStatSpec<R>

/** Wrap this regression spec so y is remapped by [expr] before the inner stat sees it. */
fun <R : Result> RegressionStatSpec<R>.transformY(expr: ScalarExpr): RegressionStatSpec<R> =
    TransformYRegression(this, expr) as RegressionStatSpec<R>

/** Wrap this regression spec so x is remapped by [expr] before the inner stat sees it. */
fun <R : Result> RegressionStatSpec<R>.transformX(expr: VectorExpr): RegressionStatSpec<R> =
    TransformXRegression(this, expr) as RegressionStatSpec<R>

/** Wrap this regression spec so every update uses [weight] regardless of caller input. */
fun <R : Result> RegressionStatSpec<R>.withWeight(weight: Double): RegressionStatSpec<R> =
    WithWeightRegression(this, weight) as RegressionStatSpec<R>

/** Wrap this regression spec so every update's weight is multiplied by `expr.eval(0, y, v)`. */
fun <R : Result> RegressionStatSpec<R>.weightBy(expr: ScalarExpr): RegressionStatSpec<R> =
    WeightByValueRegression(this, expr) as RegressionStatSpec<R>

/** Wrap this regression spec so it only sees one in every [every] updates. */
fun <R : Result> RegressionStatSpec<R>.throttle(every: Int): RegressionStatSpec<R> =
    ThrottleRegression(this, every) as RegressionStatSpec<R>

/** Wrap this regression spec to keep each update with probability [rate]; [seed] feeds the PRNG. */
fun <R : Result> RegressionStatSpec<R>.sample(rate: Double, seed: Long): RegressionStatSpec<R> =
    SampleRegression(this, rate, seed) as RegressionStatSpec<R>

/** Lift this series spec into the regression modality. [project] reduces each `(x = V, y = Y)`
 *  update to a scalar that the inner series stat absorbs. Use `Y` for the marginal-y view. */
fun <R : Result> SeriesStatSpec<R>.foldRegression(featureSize: Int, project: ScalarExpr): RegressionStatSpec<R> =
    FoldRegression(this, featureSize, project) as RegressionStatSpec<R>
