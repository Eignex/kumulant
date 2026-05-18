@file:Suppress("UNCHECKED_CAST")

package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.ResultList
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Wire-friendly counterparts of the composable operations in
 * `com.eignex.kumulant.operation.*`. Each spec holds an inner [StatSpec]
 * (polymorphic by `@SerialName`) plus the operation's primitive parameters.
 *
 * The inner field is typed at the base [StatSpec] interface so the
 * polymorphic decoder doesn't need a generic argument; construction
 * (`StatFactory.kt`) runtime-checks the inner's modality and casts it. The
 * user-facing typed surface is the extension functions below on the
 * modality-specific spec interfaces (`SeriesStatSpec<R>.withWeight(...)`,
 * etc.) - the unsafe cast there is correct because the wrapper is
 * parametric in `R` only at the type level.
 *
 * Lambda-bound operations (`filter`, `mapResult`, `transformValue`/`Pair`/`Vector`/`Long`,
 * `foldVector`/`Paired`) do not appear here - their behavior cannot be
 * expressed without an expression language. Use the live-stat back-door for
 * those.
 */

// ========== withWeight ==========

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

// ========== withValue (Series, Discrete) ==========

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

// ========== Type adapters: asSeries / asDiscrete ==========

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
fun <R : Result> DiscreteStatSpec<R>.asSeries(): SeriesStatSpec<R> =
    AsSeries(this) as SeriesStatSpec<R>

/** Adapt a series spec into a discrete spec - the discrete sees `value.toLong()` per update. */
fun <R : Result> SeriesStatSpec<R>.asDiscrete(): DiscreteStatSpec<R> =
    AsDiscrete(this) as DiscreteStatSpec<R>

// ========== Selectors: atX / atY / atIndex / atIndices ==========

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
fun <R : Result> SeriesStatSpec<R>.atIndex(index: Int): VectorStatSpec<R> =
    AtIndex(this, index) as VectorStatSpec<R>

/** Adapt a paired spec into a vector spec by consuming the [indexX] / [indexY] coordinates. */
fun <R : Result> PairedStatSpec<R>.atIndices(indexX: Int, indexY: Int): VectorStatSpec<R> =
    AtIndices(this, indexX, indexY) as VectorStatSpec<R>

// ========== Axis bindings: withFixedX/Y, withTimeAsX/Y ==========

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
fun <R : Result> PairedStatSpec<R>.withTimeAsX(): SeriesStatSpec<R> =
    WithTimeAsX(this) as SeriesStatSpec<R>

/** Adapt a paired spec into a series spec by using the update timestamp as `y`. */
fun <R : Result> PairedStatSpec<R>.withTimeAsY(): SeriesStatSpec<R> =
    WithTimeAsY(this) as SeriesStatSpec<R>

// ========== Windowed ==========

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
fun <R : Result> SeriesStatSpec<R>.windowed(
    durationMillis: Long,
    slices: Int = 10,
): SeriesStatSpec<R> = WindowedSeries(this, durationMillis, slices) as SeriesStatSpec<R>

/** Wrap this paired spec in a sliding time window of [durationMillis] split into [slices] buckets. */
fun <R : Result> PairedStatSpec<R>.windowed(
    durationMillis: Long,
    slices: Int = 10,
): PairedStatSpec<R> = WindowedPaired(this, durationMillis, slices) as PairedStatSpec<R>

/** Wrap this vector spec in a sliding time window of [durationMillis] split into [slices] buckets. */
fun <R : Result> VectorStatSpec<R>.windowed(
    durationMillis: Long,
    slices: Int = 10,
): VectorStatSpec<R> = WindowedVector(this, durationMillis, slices) as VectorStatSpec<R>

/** Wrap this discrete spec in a sliding time window of [durationMillis] split into [slices] buckets. */
fun <R : Result> DiscreteStatSpec<R>.windowed(
    durationMillis: Long,
    slices: Int = 10,
): DiscreteStatSpec<R> = WindowedDiscrete(this, durationMillis, slices) as DiscreteStatSpec<R>

// ========== Vectorized (Series template replicated per dimension) ==========

/** Wire spec for `SeriesStat.vectorized(dimensions)`: replicates a series stat across [dimensions] coordinates. */
@Serializable
@SerialName("Vectorized")
data class Vectorized(
    /** Number of vector dimensions; one [template] instance is materialised per dimension. */
    val dimensions: Int,
    /** Series spec replicated independently across every dimension. */
    val template: StatSpec,
) : VectorStatSpec<ResultList<Result>>

/** Lift a series spec to a vector spec by replicating it across every coordinate of an N-dim input. */
fun <R : Result> SeriesStatSpec<R>.vectorized(dimensions: Int): VectorStatSpec<ResultList<R>> =
    Vectorized(dimensions, this) as VectorStatSpec<ResultList<R>>

// ========== Transform / Filter via expression AST ==========

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
fun <R : Result> PairedStatSpec<R>.transformX(expr: ScalarExpr): PairedStatSpec<R> =
    transformPair(expr, Y)

/** Map only the y coordinate; x stays as-is. */
fun <R : Result> PairedStatSpec<R>.transformY(expr: ScalarExpr): PairedStatSpec<R> =
    transformPair(X, expr)

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
