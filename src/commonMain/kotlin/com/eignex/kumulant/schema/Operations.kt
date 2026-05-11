@file:Suppress("UNCHECKED_CAST")

package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.operation.FilterDiscreteStat
import com.eignex.kumulant.operation.FilterPairedStat
import com.eignex.kumulant.operation.FilterSeriesStat
import com.eignex.kumulant.operation.FilterVectorStat
import com.eignex.kumulant.operation.FoldPairedStat
import com.eignex.kumulant.operation.FoldVectorStat
import com.eignex.kumulant.operation.TransformLongStat
import com.eignex.kumulant.operation.TransformPairStat
import com.eignex.kumulant.operation.TransformValueStat
import com.eignex.kumulant.operation.TransformVectorStat
import com.eignex.kumulant.operation.VectorizedStat
import com.eignex.kumulant.operation.asDiscrete
import com.eignex.kumulant.operation.asSeries
import com.eignex.kumulant.operation.atIndex
import com.eignex.kumulant.operation.atIndices
import com.eignex.kumulant.operation.atX
import com.eignex.kumulant.operation.atY
import com.eignex.kumulant.operation.windowed
import com.eignex.kumulant.operation.withFixedX
import com.eignex.kumulant.operation.withFixedY
import com.eignex.kumulant.operation.withTimeAsX
import com.eignex.kumulant.operation.withTimeAsY
import com.eignex.kumulant.operation.withValue
import com.eignex.kumulant.operation.withWeight
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.time.Duration.Companion.milliseconds

/**
 * Wire-friendly counterparts of the composable operations in
 * `com.eignex.kumulant.operation.*`. Each spec holds an inner [StatSpec]
 * (polymorphic by `@SerialName`) plus the operation's primitive parameters.
 *
 * The inner field is typed at the base [StatSpec] interface so the
 * polymorphic decoder doesn't need a generic argument; `materialize` runtime-
 * checks the inner's modality and casts it. The user-facing typed surface is
 * the inline extension functions on the modality-specific spec interfaces
 * (`SeriesStatSpec<R>.withWeight(...)`, etc.) — the unsafe cast there is
 * correct because the wrapper is parametric in `R` only at the type level.
 *
 * Lambda-bound operations (`filter`, `mapResult`, `transformValue`/`Pair`/`Vector`/`Long`,
 * `foldVector`/`Paired`) do not appear here — their behavior cannot be
 * expressed without an expression language. Use the live-stat back-door for
 * those.
 */

private fun requireSeries(inner: StatSpec, op: String): SeriesStatSpec<*> {
    require(inner is SeriesStatSpec<*>) {
        "$op expects a SeriesStatSpec inner; got ${inner::class.simpleName}"
    }
    return inner
}

private fun requirePaired(inner: StatSpec, op: String): PairedStatSpec<*> {
    require(inner is PairedStatSpec<*>) {
        "$op expects a PairedStatSpec inner; got ${inner::class.simpleName}"
    }
    return inner
}

private fun requireVector(inner: StatSpec, op: String): VectorStatSpec<*> {
    require(inner is VectorStatSpec<*>) {
        "$op expects a VectorStatSpec inner; got ${inner::class.simpleName}"
    }
    return inner
}

private fun requireDiscrete(inner: StatSpec, op: String): DiscreteStatSpec<*> {
    require(inner is DiscreteStatSpec<*>) {
        "$op expects a DiscreteStatSpec inner; got ${inner::class.simpleName}"
    }
    return inner
}

// ========== withWeight ==========

@Serializable
@SerialName("WithWeightSeries")
data class WithWeightSeries(val inner: StatSpec, val weight: Double) : SeriesStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): SeriesStat<Result> {
        val materialized = requireSeries(inner, "WithWeightSeries").materialize(concurrency) as SeriesStat<Result>
        return materialized.withWeight(weight)
    }
}

@Serializable
@SerialName("WithWeightPaired")
data class WithWeightPaired(val inner: StatSpec, val weight: Double) : PairedStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): PairedStat<Result> {
        val materialized = requirePaired(inner, "WithWeightPaired").materialize(concurrency) as PairedStat<Result>
        return materialized.withWeight(weight)
    }
}

@Serializable
@SerialName("WithWeightVector")
data class WithWeightVector(val inner: StatSpec, val weight: Double) : VectorStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): VectorStat<Result> {
        val materialized = requireVector(inner, "WithWeightVector").materialize(concurrency) as VectorStat<Result>
        return materialized.withWeight(weight)
    }
}

@Serializable
@SerialName("WithWeightDiscrete")
data class WithWeightDiscrete(val inner: StatSpec, val weight: Double) : DiscreteStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): DiscreteStat<Result> {
        val materialized = requireDiscrete(inner, "WithWeightDiscrete").materialize(concurrency) as DiscreteStat<Result>
        return materialized.withWeight(weight)
    }
}

fun <R : Result> SeriesStatSpec<R>.withWeight(weight: Double): SeriesStatSpec<R> =
    WithWeightSeries(this, weight) as SeriesStatSpec<R>

fun <R : Result> PairedStatSpec<R>.withWeight(weight: Double): PairedStatSpec<R> =
    WithWeightPaired(this, weight) as PairedStatSpec<R>

fun <R : Result> VectorStatSpec<R>.withWeight(weight: Double): VectorStatSpec<R> =
    WithWeightVector(this, weight) as VectorStatSpec<R>

fun <R : Result> DiscreteStatSpec<R>.withWeight(weight: Double): DiscreteStatSpec<R> =
    WithWeightDiscrete(this, weight) as DiscreteStatSpec<R>

// ========== withValue (Series, Discrete) ==========

@Serializable
@SerialName("WithValueSeries")
data class WithValueSeries(val inner: StatSpec, val value: Double) : SeriesStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): SeriesStat<Result> {
        val materialized = requireSeries(inner, "WithValueSeries").materialize(concurrency) as SeriesStat<Result>
        return materialized.withValue(value)
    }
}

@Serializable
@SerialName("WithValueDiscrete")
data class WithValueDiscrete(val inner: StatSpec, val value: Long) : DiscreteStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): DiscreteStat<Result> {
        val materialized = requireDiscrete(inner, "WithValueDiscrete").materialize(concurrency) as DiscreteStat<Result>
        return materialized.withValue(value)
    }
}

fun <R : Result> SeriesStatSpec<R>.withValue(value: Double): SeriesStatSpec<R> =
    WithValueSeries(this, value) as SeriesStatSpec<R>

fun <R : Result> DiscreteStatSpec<R>.withValue(value: Long): DiscreteStatSpec<R> =
    WithValueDiscrete(this, value) as DiscreteStatSpec<R>

// ========== Type adapters: asSeries / asDiscrete ==========

@Serializable
@SerialName("AsSeries")
data class AsSeries(val inner: StatSpec) : SeriesStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): SeriesStat<Result> {
        val materialized = requireDiscrete(inner, "AsSeries").materialize(concurrency) as DiscreteStat<Result>
        return materialized.asSeries()
    }
}

@Serializable
@SerialName("AsDiscrete")
data class AsDiscrete(val inner: StatSpec) : DiscreteStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): DiscreteStat<Result> {
        val materialized = requireSeries(inner, "AsDiscrete").materialize(concurrency) as SeriesStat<Result>
        return materialized.asDiscrete()
    }
}

fun <R : Result> DiscreteStatSpec<R>.asSeries(): SeriesStatSpec<R> =
    AsSeries(this) as SeriesStatSpec<R>

fun <R : Result> SeriesStatSpec<R>.asDiscrete(): DiscreteStatSpec<R> =
    AsDiscrete(this) as DiscreteStatSpec<R>

// ========== Selectors: atX / atY / atIndex / atIndices ==========

@Serializable
@SerialName("AtX")
data class AtX(val inner: StatSpec) : PairedStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): PairedStat<Result> {
        val materialized = requireSeries(inner, "AtX").materialize(concurrency) as SeriesStat<Result>
        return materialized.atX()
    }
}

@Serializable
@SerialName("AtY")
data class AtY(val inner: StatSpec) : PairedStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): PairedStat<Result> {
        val materialized = requireSeries(inner, "AtY").materialize(concurrency) as SeriesStat<Result>
        return materialized.atY()
    }
}

@Serializable
@SerialName("AtIndex")
data class AtIndex(val inner: StatSpec, val index: Int) : VectorStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): VectorStat<Result> {
        val materialized = requireSeries(inner, "AtIndex").materialize(concurrency) as SeriesStat<Result>
        return materialized.atIndex(index)
    }
}

@Serializable
@SerialName("AtIndices")
data class AtIndices(val inner: StatSpec, val indexX: Int, val indexY: Int) : VectorStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): VectorStat<Result> {
        val materialized = requirePaired(inner, "AtIndices").materialize(concurrency) as PairedStat<Result>
        return materialized.atIndices(indexX, indexY)
    }
}

fun <R : Result> SeriesStatSpec<R>.atX(): PairedStatSpec<R> = AtX(this) as PairedStatSpec<R>

fun <R : Result> SeriesStatSpec<R>.atY(): PairedStatSpec<R> = AtY(this) as PairedStatSpec<R>

fun <R : Result> SeriesStatSpec<R>.atIndex(index: Int): VectorStatSpec<R> =
    AtIndex(this, index) as VectorStatSpec<R>

fun <R : Result> PairedStatSpec<R>.atIndices(indexX: Int, indexY: Int): VectorStatSpec<R> =
    AtIndices(this, indexX, indexY) as VectorStatSpec<R>

// ========== Axis bindings: withFixedX/Y, withTimeAsX/Y ==========

@Serializable
@SerialName("WithFixedX")
data class WithFixedX(val inner: StatSpec, val fixedX: Double) : SeriesStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): SeriesStat<Result> {
        val materialized = requirePaired(inner, "WithFixedX").materialize(concurrency) as PairedStat<Result>
        return materialized.withFixedX(fixedX)
    }
}

@Serializable
@SerialName("WithFixedY")
data class WithFixedY(val inner: StatSpec, val fixedY: Double) : SeriesStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): SeriesStat<Result> {
        val materialized = requirePaired(inner, "WithFixedY").materialize(concurrency) as PairedStat<Result>
        return materialized.withFixedY(fixedY)
    }
}

@Serializable
@SerialName("WithTimeAsX")
data class WithTimeAsX(val inner: StatSpec) : SeriesStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): SeriesStat<Result> {
        val materialized = requirePaired(inner, "WithTimeAsX").materialize(concurrency) as PairedStat<Result>
        return materialized.withTimeAsX()
    }
}

@Serializable
@SerialName("WithTimeAsY")
data class WithTimeAsY(val inner: StatSpec) : SeriesStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): SeriesStat<Result> {
        val materialized = requirePaired(inner, "WithTimeAsY").materialize(concurrency) as PairedStat<Result>
        return materialized.withTimeAsY()
    }
}

fun <R : Result> PairedStatSpec<R>.withFixedX(fixedX: Double): SeriesStatSpec<R> =
    WithFixedX(this, fixedX) as SeriesStatSpec<R>

fun <R : Result> PairedStatSpec<R>.withFixedY(fixedY: Double): SeriesStatSpec<R> =
    WithFixedY(this, fixedY) as SeriesStatSpec<R>

fun <R : Result> PairedStatSpec<R>.withTimeAsX(): SeriesStatSpec<R> =
    WithTimeAsX(this) as SeriesStatSpec<R>

fun <R : Result> PairedStatSpec<R>.withTimeAsY(): SeriesStatSpec<R> =
    WithTimeAsY(this) as SeriesStatSpec<R>

// ========== Windowed ==========

@Serializable
@SerialName("WindowedSeries")
data class WindowedSeries(
    val inner: StatSpec,
    val durationMillis: Long,
    val slices: Int = 10,
) : SeriesStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): SeriesStat<Result> {
        val materialized = requireSeries(inner, "WindowedSeries").materialize(concurrency) as SeriesStat<Result>
        return materialized.windowed(durationMillis.milliseconds, slices, concurrency)
    }
}

@Serializable
@SerialName("WindowedPaired")
data class WindowedPaired(
    val inner: StatSpec,
    val durationMillis: Long,
    val slices: Int = 10,
) : PairedStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): PairedStat<Result> {
        val materialized = requirePaired(inner, "WindowedPaired").materialize(concurrency) as PairedStat<Result>
        return materialized.windowed(durationMillis.milliseconds, slices, concurrency)
    }
}

@Serializable
@SerialName("WindowedVector")
data class WindowedVector(
    val inner: StatSpec,
    val durationMillis: Long,
    val slices: Int = 10,
) : VectorStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): VectorStat<Result> {
        val materialized = requireVector(inner, "WindowedVector").materialize(concurrency) as VectorStat<Result>
        return materialized.windowed(durationMillis.milliseconds, slices, concurrency)
    }
}

@Serializable
@SerialName("WindowedDiscrete")
data class WindowedDiscrete(
    val inner: StatSpec,
    val durationMillis: Long,
    val slices: Int = 10,
) : DiscreteStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): DiscreteStat<Result> {
        val materialized = requireDiscrete(inner, "WindowedDiscrete").materialize(concurrency) as DiscreteStat<Result>
        return materialized.windowed(durationMillis.milliseconds, slices, concurrency)
    }
}

fun <R : Result> SeriesStatSpec<R>.windowed(
    durationMillis: Long,
    slices: Int = 10,
): SeriesStatSpec<R> = WindowedSeries(this, durationMillis, slices) as SeriesStatSpec<R>

fun <R : Result> PairedStatSpec<R>.windowed(
    durationMillis: Long,
    slices: Int = 10,
): PairedStatSpec<R> = WindowedPaired(this, durationMillis, slices) as PairedStatSpec<R>

fun <R : Result> VectorStatSpec<R>.windowed(
    durationMillis: Long,
    slices: Int = 10,
): VectorStatSpec<R> = WindowedVector(this, durationMillis, slices) as VectorStatSpec<R>

fun <R : Result> DiscreteStatSpec<R>.windowed(
    durationMillis: Long,
    slices: Int = 10,
): DiscreteStatSpec<R> = WindowedDiscrete(this, durationMillis, slices) as DiscreteStatSpec<R>

// ========== Vectorized (Series template replicated per dimension) ==========

@Serializable
@SerialName("Vectorized")
data class VectorizedStat(val dimensions: Int, val template: StatSpec) :
    VectorStatSpec<ResultList<Result>> {
    override fun materialize(concurrency: Concurrency): VectorStat<ResultList<Result>> {
        val tpl = requireSeries(template, "Vectorized").materialize(concurrency) as SeriesStat<Result>
        return VectorizedStat(dimensions, tpl, concurrency)
    }
}

fun <R : Result> SeriesStatSpec<R>.vectorized(dimensions: Int): VectorStatSpec<ResultList<R>> =
    VectorizedStat(dimensions, this) as VectorStatSpec<ResultList<R>>

// ========== Transform / Filter via expression AST ==========

/**
 * Apply [expr] as the value transform on every update — wire-friendly
 * counterpart of `SeriesStat<R>.transformValue { … }`. The Kotlin lambda is
 * built at materialize time and forwards every input through `expr.eval`;
 * the AST itself ([ScalarExpr]) is what travels on the wire.
 */
@Serializable
@SerialName("TransformValueSeries")
data class TransformValueSeries(val inner: StatSpec, val expr: ScalarExpr) : SeriesStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): SeriesStat<Result> {
        val materialized = requireSeries(inner, "TransformValueSeries").materialize(concurrency) as SeriesStat<Result>
        return TransformValueStat(materialized) { expr.eval(it) }
    }
}

@Serializable
@SerialName("TransformValueDiscrete")
data class TransformValueDiscrete(val inner: StatSpec, val expr: ScalarExpr) : DiscreteStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): DiscreteStat<Result> {
        val materialized = requireDiscrete(
            inner,
            "TransformValueDiscrete"
        ).materialize(concurrency) as DiscreteStat<Result>
        return TransformLongStat(materialized) { expr.eval(it.toDouble()).toLong() }
    }
}

@Serializable
@SerialName("FilterValueSeries")
data class FilterValueSeries(val inner: StatSpec, val pred: BoolExpr) : SeriesStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): SeriesStat<Result> {
        val materialized = requireSeries(inner, "FilterValueSeries").materialize(concurrency) as SeriesStat<Result>
        return FilterSeriesStat(materialized) { pred.eval(it) }
    }
}

@Serializable
@SerialName("FilterValueDiscrete")
data class FilterValueDiscrete(val inner: StatSpec, val pred: BoolExpr) : DiscreteStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): DiscreteStat<Result> {
        val materialized = requireDiscrete(
            inner,
            "FilterValueDiscrete"
        ).materialize(concurrency) as DiscreteStat<Result>
        return FilterDiscreteStat(materialized) { pred.eval(it.toDouble()) }
    }
}

fun <R : Result> SeriesStatSpec<R>.transform(expr: ScalarExpr): SeriesStatSpec<R> =
    TransformValueSeries(this, expr) as SeriesStatSpec<R>

fun <R : Result> DiscreteStatSpec<R>.transform(expr: ScalarExpr): DiscreteStatSpec<R> =
    TransformValueDiscrete(this, expr) as DiscreteStatSpec<R>

fun <R : Result> SeriesStatSpec<R>.filter(pred: BoolExpr): SeriesStatSpec<R> =
    FilterValueSeries(this, pred) as SeriesStatSpec<R>

fun <R : Result> DiscreteStatSpec<R>.filter(pred: BoolExpr): DiscreteStatSpec<R> =
    FilterValueDiscrete(this, pred) as DiscreteStatSpec<R>

/**
 * Apply [xExpr] / [yExpr] to map each `(x, y)` pair before update — wire-friendly
 * counterpart of `PairedStat<R>.transformPair { x, y -> … }`. Each expr can
 * reference both [X] and [Y] of the original input.
 */
@Serializable
@SerialName("TransformPair")
data class TransformPair(
    val inner: StatSpec,
    val xExpr: ScalarExpr,
    val yExpr: ScalarExpr,
) : PairedStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): PairedStat<Result> {
        val materialized = requirePaired(inner, "TransformPair").materialize(concurrency) as PairedStat<Result>
        return TransformPairStat(materialized) { xv, yv -> xExpr.eval(xv, yv) to yExpr.eval(xv, yv) }
    }
}

@Serializable
@SerialName("FilterPaired")
data class FilterPaired(val inner: StatSpec, val pred: BoolExpr) : PairedStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): PairedStat<Result> {
        val materialized = requirePaired(inner, "FilterPaired").materialize(concurrency) as PairedStat<Result>
        return FilterPairedStat(materialized) { xv, yv -> pred.eval(xv, yv) }
    }
}

fun <R : Result> PairedStatSpec<R>.transformPair(xExpr: ScalarExpr, yExpr: ScalarExpr): PairedStatSpec<R> =
    TransformPair(this, xExpr, yExpr) as PairedStatSpec<R>

/** Map only the x coordinate; y stays as-is. */
fun <R : Result> PairedStatSpec<R>.transformX(expr: ScalarExpr): PairedStatSpec<R> =
    transformPair(expr, Y)

/** Map only the y coordinate; x stays as-is. */
fun <R : Result> PairedStatSpec<R>.transformY(expr: ScalarExpr): PairedStatSpec<R> =
    transformPair(X, expr)

fun <R : Result> PairedStatSpec<R>.filter(pred: BoolExpr): PairedStatSpec<R> =
    FilterPaired(this, pred) as PairedStatSpec<R>

/**
 * Apply [expr] element-wise to every entry of the incoming vector before
 * update. The expression sees the current element as [X] and can reference
 * any other element via [V]`(j)` — sufficient for normalization,
 * standardization, masking, etc. For arbitrary cross-element vector→vector
 * transforms beyond per-element evaluation, use the live `transformVector`
 * with a Kotlin lambda.
 */
@Serializable
@SerialName("TransformVectorElement")
data class TransformVectorElement(val inner: StatSpec, val expr: ScalarExpr) : VectorStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): VectorStat<Result> {
        val materialized = requireVector(inner, "TransformVectorElement").materialize(concurrency) as VectorStat<Result>
        return TransformVectorStat(materialized) { vec ->
            DoubleArray(vec.size) { i -> expr.eval(vec[i], 0.0, vec) }
        }
    }
}

@Serializable
@SerialName("FilterVector")
data class FilterVector(val inner: StatSpec, val pred: BoolExpr) : VectorStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): VectorStat<Result> {
        val materialized = requireVector(inner, "FilterVector").materialize(concurrency) as VectorStat<Result>
        return FilterVectorStat(materialized) { vec -> pred.eval(0.0, 0.0, vec) }
    }
}

fun <R : Result> VectorStatSpec<R>.transformElement(expr: ScalarExpr): VectorStatSpec<R> =
    TransformVectorElement(this, expr) as VectorStatSpec<R>

fun <R : Result> VectorStatSpec<R>.filter(pred: BoolExpr): VectorStatSpec<R> =
    FilterVector(this, pred) as VectorStatSpec<R>

/**
 * Lift a [SeriesStatSpec] to a [PairedStatSpec] by reducing each `(x, y)`
 * pair to a scalar via [expr] before driving the inner stat. The expression
 * is free to reference both [X] and [Y].
 */
@Serializable
@SerialName("FoldPaired")
data class FoldPaired(val inner: StatSpec, val expr: ScalarExpr) : PairedStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): PairedStat<Result> {
        val materialized = requireSeries(inner, "FoldPaired").materialize(concurrency) as SeriesStat<Result>
        return FoldPairedStat(materialized) { xv, yv -> expr.eval(xv, yv) }
    }
}

/**
 * Lift a [SeriesStatSpec] to a [VectorStatSpec] by reducing each vector to
 * a scalar via [expr] before driving the inner stat. The expression typically
 * uses [VFold] / [VDot] / [V] to consume the vector.
 */
@Serializable
@SerialName("FoldVector")
data class FoldVector(val inner: StatSpec, val expr: ScalarExpr) : VectorStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): VectorStat<Result> {
        val materialized = requireSeries(inner, "FoldVector").materialize(concurrency) as SeriesStat<Result>
        return FoldVectorStat(materialized) { vec -> expr.eval(0.0, 0.0, vec) }
    }
}

fun <R : Result> SeriesStatSpec<R>.foldPaired(expr: ScalarExpr): PairedStatSpec<R> =
    FoldPaired(this, expr) as PairedStatSpec<R>

fun <R : Result> SeriesStatSpec<R>.foldVector(expr: ScalarExpr): VectorStatSpec<R> =
    FoldVector(this, expr) as VectorStatSpec<R>

/**
 * Apply a [VectorExpr] to remap each incoming vector before update — wire
 * counterpart of `VectorStat<R>.transformVector { … }`. Output length and
 * input length need not match; the inner stat must be parameterised for the
 * output dim.
 */
@Serializable
@SerialName("TransformVector")
data class TransformVector(val inner: StatSpec, val expr: VectorExpr) : VectorStatSpec<Result> {
    override fun materialize(concurrency: Concurrency): VectorStat<Result> {
        val materialized = requireVector(inner, "TransformVector").materialize(concurrency) as VectorStat<Result>
        return TransformVectorStat(materialized) { vec -> expr.eval(0.0, 0.0, vec) }
    }
}

fun <R : Result> VectorStatSpec<R>.transformVector(expr: VectorExpr): VectorStatSpec<R> =
    TransformVector(this, expr) as VectorStatSpec<R>
