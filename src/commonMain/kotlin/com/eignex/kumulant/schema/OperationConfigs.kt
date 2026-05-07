package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.ResultList
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.operation.VectorizedStat
import com.eignex.kumulant.operation.asDiscrete
import com.eignex.kumulant.operation.asSeries
import com.eignex.kumulant.operation.atIndex
import com.eignex.kumulant.operation.atIndices
import com.eignex.kumulant.operation.atX
import com.eignex.kumulant.operation.atY
import com.eignex.kumulant.operation.filter
import com.eignex.kumulant.operation.transformValue
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
 * `com.eignex.kumulant.operation.*`. Each config holds an inner [StatConfig]
 * (polymorphic via `$type`) plus the operation's primitive parameters.
 *
 * The inner field is typed at the base [StatConfig] interface so the
 * polymorphic decoder doesn't need a generic argument; `materialize` runtime-
 * checks the inner's modality and casts it. The user-facing typed surface is
 * the inline extension functions on the modality-specific config interfaces
 * (`SeriesStatConfig<R>.withWeight(...)`, etc.) — the unsafe cast there is
 * correct because the wrapper is parametric in `R` only at the type level.
 *
 * Lambda-bound operations (`filter`, `mapResult`, `transformValue`/`Pair`/`Vector`/`Long`,
 * `foldVector`/`Paired`) do not appear here — their behavior cannot be
 * expressed without an expression language. Use the live-stat back-door for
 * those.
 */

private fun requireSeries(inner: StatConfig, op: String): SeriesStatConfig<*> {
    require(inner is SeriesStatConfig<*>) {
        "$op expects a SeriesStatConfig inner; got ${inner::class.simpleName}"
    }
    return inner
}

private fun requirePaired(inner: StatConfig, op: String): PairedStatConfig<*> {
    require(inner is PairedStatConfig<*>) {
        "$op expects a PairedStatConfig inner; got ${inner::class.simpleName}"
    }
    return inner
}

private fun requireVector(inner: StatConfig, op: String): VectorStatConfig<*> {
    require(inner is VectorStatConfig<*>) {
        "$op expects a VectorStatConfig inner; got ${inner::class.simpleName}"
    }
    return inner
}

private fun requireDiscrete(inner: StatConfig, op: String): DiscreteStatConfig<*> {
    require(inner is DiscreteStatConfig<*>) {
        "$op expects a DiscreteStatConfig inner; got ${inner::class.simpleName}"
    }
    return inner
}

// ========== withWeight ==========

@Serializable @SerialName("WithWeightSeries")
data class WithWeightSeriesConfig(val inner: StatConfig, val weight: Double) : SeriesStatConfig<Result> {
    override fun materialize(concurrency: Concurrency): SeriesStat<Result> {
        @Suppress("UNCHECKED_CAST")
        val materialized = requireSeries(inner, "WithWeightSeries").materialize(concurrency) as SeriesStat<Result>
        return materialized.withWeight(weight)
    }
}

@Serializable @SerialName("WithWeightPaired")
data class WithWeightPairedConfig(val inner: StatConfig, val weight: Double) : PairedStatConfig<Result> {
    override fun materialize(concurrency: Concurrency): PairedStat<Result> {
        @Suppress("UNCHECKED_CAST")
        val materialized = requirePaired(inner, "WithWeightPaired").materialize(concurrency) as PairedStat<Result>
        return materialized.withWeight(weight)
    }
}

@Serializable @SerialName("WithWeightVector")
data class WithWeightVectorConfig(val inner: StatConfig, val weight: Double) : VectorStatConfig<Result> {
    override fun materialize(concurrency: Concurrency): VectorStat<Result> {
        @Suppress("UNCHECKED_CAST")
        val materialized = requireVector(inner, "WithWeightVector").materialize(concurrency) as VectorStat<Result>
        return materialized.withWeight(weight)
    }
}

@Serializable @SerialName("WithWeightDiscrete")
data class WithWeightDiscreteConfig(val inner: StatConfig, val weight: Double) : DiscreteStatConfig<Result> {
    override fun materialize(concurrency: Concurrency): DiscreteStat<Result> {
        @Suppress("UNCHECKED_CAST")
        val materialized = requireDiscrete(inner, "WithWeightDiscrete").materialize(concurrency) as DiscreteStat<Result>
        return materialized.withWeight(weight)
    }
}

@Suppress("UNCHECKED_CAST")
fun <R : Result> SeriesStatConfig<R>.withWeight(weight: Double): SeriesStatConfig<R> =
    WithWeightSeriesConfig(this, weight) as SeriesStatConfig<R>

@Suppress("UNCHECKED_CAST")
fun <R : Result> PairedStatConfig<R>.withWeight(weight: Double): PairedStatConfig<R> =
    WithWeightPairedConfig(this, weight) as PairedStatConfig<R>

@Suppress("UNCHECKED_CAST")
fun <R : Result> VectorStatConfig<R>.withWeight(weight: Double): VectorStatConfig<R> =
    WithWeightVectorConfig(this, weight) as VectorStatConfig<R>

@Suppress("UNCHECKED_CAST")
fun <R : Result> DiscreteStatConfig<R>.withWeight(weight: Double): DiscreteStatConfig<R> =
    WithWeightDiscreteConfig(this, weight) as DiscreteStatConfig<R>

// ========== withValue (Series, Discrete) ==========

@Serializable @SerialName("WithValueSeries")
data class WithValueSeriesConfig(val inner: StatConfig, val value: Double) : SeriesStatConfig<Result> {
    override fun materialize(concurrency: Concurrency): SeriesStat<Result> {
        @Suppress("UNCHECKED_CAST")
        val materialized = requireSeries(inner, "WithValueSeries").materialize(concurrency) as SeriesStat<Result>
        return materialized.withValue(value)
    }
}

@Serializable @SerialName("WithValueDiscrete")
data class WithValueDiscreteConfig(val inner: StatConfig, val value: Long) : DiscreteStatConfig<Result> {
    override fun materialize(concurrency: Concurrency): DiscreteStat<Result> {
        @Suppress("UNCHECKED_CAST")
        val materialized = requireDiscrete(inner, "WithValueDiscrete").materialize(concurrency) as DiscreteStat<Result>
        return materialized.withValue(value)
    }
}

@Suppress("UNCHECKED_CAST")
fun <R : Result> SeriesStatConfig<R>.withValue(value: Double): SeriesStatConfig<R> =
    WithValueSeriesConfig(this, value) as SeriesStatConfig<R>

@Suppress("UNCHECKED_CAST")
fun <R : Result> DiscreteStatConfig<R>.withValue(value: Long): DiscreteStatConfig<R> =
    WithValueDiscreteConfig(this, value) as DiscreteStatConfig<R>

// ========== Type adapters: asSeries / asDiscrete ==========

@Serializable @SerialName("AsSeries")
data class AsSeriesConfig(val inner: StatConfig) : SeriesStatConfig<Result> {
    override fun materialize(concurrency: Concurrency): SeriesStat<Result> {
        @Suppress("UNCHECKED_CAST")
        val materialized = requireDiscrete(inner, "AsSeries").materialize(concurrency) as DiscreteStat<Result>
        return materialized.asSeries()
    }
}

@Serializable @SerialName("AsDiscrete")
data class AsDiscreteConfig(val inner: StatConfig) : DiscreteStatConfig<Result> {
    override fun materialize(concurrency: Concurrency): DiscreteStat<Result> {
        @Suppress("UNCHECKED_CAST")
        val materialized = requireSeries(inner, "AsDiscrete").materialize(concurrency) as SeriesStat<Result>
        return materialized.asDiscrete()
    }
}

@Suppress("UNCHECKED_CAST")
fun <R : Result> DiscreteStatConfig<R>.asSeries(): SeriesStatConfig<R> =
    AsSeriesConfig(this) as SeriesStatConfig<R>

@Suppress("UNCHECKED_CAST")
fun <R : Result> SeriesStatConfig<R>.asDiscrete(): DiscreteStatConfig<R> =
    AsDiscreteConfig(this) as DiscreteStatConfig<R>

// ========== Selectors: atX / atY / atIndex / atIndices ==========

@Serializable @SerialName("AtX")
data class AtXConfig(val inner: StatConfig) : PairedStatConfig<Result> {
    override fun materialize(concurrency: Concurrency): PairedStat<Result> {
        @Suppress("UNCHECKED_CAST")
        val materialized = requireSeries(inner, "AtX").materialize(concurrency) as SeriesStat<Result>
        return materialized.atX()
    }
}

@Serializable @SerialName("AtY")
data class AtYConfig(val inner: StatConfig) : PairedStatConfig<Result> {
    override fun materialize(concurrency: Concurrency): PairedStat<Result> {
        @Suppress("UNCHECKED_CAST")
        val materialized = requireSeries(inner, "AtY").materialize(concurrency) as SeriesStat<Result>
        return materialized.atY()
    }
}

@Serializable @SerialName("AtIndex")
data class AtIndexConfig(val inner: StatConfig, val index: Int) : VectorStatConfig<Result> {
    override fun materialize(concurrency: Concurrency): VectorStat<Result> {
        @Suppress("UNCHECKED_CAST")
        val materialized = requireSeries(inner, "AtIndex").materialize(concurrency) as SeriesStat<Result>
        return materialized.atIndex(index)
    }
}

@Serializable @SerialName("AtIndices")
data class AtIndicesConfig(val inner: StatConfig, val indexX: Int, val indexY: Int) : VectorStatConfig<Result> {
    override fun materialize(concurrency: Concurrency): VectorStat<Result> {
        @Suppress("UNCHECKED_CAST")
        val materialized = requirePaired(inner, "AtIndices").materialize(concurrency) as PairedStat<Result>
        return materialized.atIndices(indexX, indexY)
    }
}

@Suppress("UNCHECKED_CAST")
fun <R : Result> SeriesStatConfig<R>.atX(): PairedStatConfig<R> = AtXConfig(this) as PairedStatConfig<R>

@Suppress("UNCHECKED_CAST")
fun <R : Result> SeriesStatConfig<R>.atY(): PairedStatConfig<R> = AtYConfig(this) as PairedStatConfig<R>

@Suppress("UNCHECKED_CAST")
fun <R : Result> SeriesStatConfig<R>.atIndex(index: Int): VectorStatConfig<R> =
    AtIndexConfig(this, index) as VectorStatConfig<R>

@Suppress("UNCHECKED_CAST")
fun <R : Result> PairedStatConfig<R>.atIndices(indexX: Int, indexY: Int): VectorStatConfig<R> =
    AtIndicesConfig(this, indexX, indexY) as VectorStatConfig<R>

// ========== Axis bindings: withFixedX/Y, withTimeAsX/Y ==========

@Serializable @SerialName("WithFixedX")
data class WithFixedXConfig(val inner: StatConfig, val fixedX: Double) : SeriesStatConfig<Result> {
    override fun materialize(concurrency: Concurrency): SeriesStat<Result> {
        @Suppress("UNCHECKED_CAST")
        val materialized = requirePaired(inner, "WithFixedX").materialize(concurrency) as PairedStat<Result>
        return materialized.withFixedX(fixedX)
    }
}

@Serializable @SerialName("WithFixedY")
data class WithFixedYConfig(val inner: StatConfig, val fixedY: Double) : SeriesStatConfig<Result> {
    override fun materialize(concurrency: Concurrency): SeriesStat<Result> {
        @Suppress("UNCHECKED_CAST")
        val materialized = requirePaired(inner, "WithFixedY").materialize(concurrency) as PairedStat<Result>
        return materialized.withFixedY(fixedY)
    }
}

@Serializable @SerialName("WithTimeAsX")
data class WithTimeAsXConfig(val inner: StatConfig) : SeriesStatConfig<Result> {
    override fun materialize(concurrency: Concurrency): SeriesStat<Result> {
        @Suppress("UNCHECKED_CAST")
        val materialized = requirePaired(inner, "WithTimeAsX").materialize(concurrency) as PairedStat<Result>
        return materialized.withTimeAsX()
    }
}

@Serializable @SerialName("WithTimeAsY")
data class WithTimeAsYConfig(val inner: StatConfig) : SeriesStatConfig<Result> {
    override fun materialize(concurrency: Concurrency): SeriesStat<Result> {
        @Suppress("UNCHECKED_CAST")
        val materialized = requirePaired(inner, "WithTimeAsY").materialize(concurrency) as PairedStat<Result>
        return materialized.withTimeAsY()
    }
}

@Suppress("UNCHECKED_CAST")
fun <R : Result> PairedStatConfig<R>.withFixedX(fixedX: Double): SeriesStatConfig<R> =
    WithFixedXConfig(this, fixedX) as SeriesStatConfig<R>

@Suppress("UNCHECKED_CAST")
fun <R : Result> PairedStatConfig<R>.withFixedY(fixedY: Double): SeriesStatConfig<R> =
    WithFixedYConfig(this, fixedY) as SeriesStatConfig<R>

@Suppress("UNCHECKED_CAST")
fun <R : Result> PairedStatConfig<R>.withTimeAsX(): SeriesStatConfig<R> =
    WithTimeAsXConfig(this) as SeriesStatConfig<R>

@Suppress("UNCHECKED_CAST")
fun <R : Result> PairedStatConfig<R>.withTimeAsY(): SeriesStatConfig<R> =
    WithTimeAsYConfig(this) as SeriesStatConfig<R>

// ========== Windowed ==========

@Serializable @SerialName("WindowedSeries")
data class WindowedSeriesConfig(
    val inner: StatConfig,
    val durationMillis: Long,
    val slices: Int = 10,
) : SeriesStatConfig<Result> {
    override fun materialize(concurrency: Concurrency): SeriesStat<Result> {
        @Suppress("UNCHECKED_CAST")
        val materialized = requireSeries(inner, "WindowedSeries").materialize(concurrency) as SeriesStat<Result>
        return materialized.windowed(durationMillis.milliseconds, slices, concurrency)
    }
}

@Serializable @SerialName("WindowedPaired")
data class WindowedPairedConfig(
    val inner: StatConfig,
    val durationMillis: Long,
    val slices: Int = 10,
) : PairedStatConfig<Result> {
    override fun materialize(concurrency: Concurrency): PairedStat<Result> {
        @Suppress("UNCHECKED_CAST")
        val materialized = requirePaired(inner, "WindowedPaired").materialize(concurrency) as PairedStat<Result>
        return materialized.windowed(durationMillis.milliseconds, slices, concurrency)
    }
}

@Serializable @SerialName("WindowedVector")
data class WindowedVectorConfig(
    val inner: StatConfig,
    val durationMillis: Long,
    val slices: Int = 10,
) : VectorStatConfig<Result> {
    override fun materialize(concurrency: Concurrency): VectorStat<Result> {
        @Suppress("UNCHECKED_CAST")
        val materialized = requireVector(inner, "WindowedVector").materialize(concurrency) as VectorStat<Result>
        return materialized.windowed(durationMillis.milliseconds, slices, concurrency)
    }
}

@Serializable @SerialName("WindowedDiscrete")
data class WindowedDiscreteConfig(
    val inner: StatConfig,
    val durationMillis: Long,
    val slices: Int = 10,
) : DiscreteStatConfig<Result> {
    override fun materialize(concurrency: Concurrency): DiscreteStat<Result> {
        @Suppress("UNCHECKED_CAST")
        val materialized = requireDiscrete(inner, "WindowedDiscrete").materialize(concurrency) as DiscreteStat<Result>
        return materialized.windowed(durationMillis.milliseconds, slices, concurrency)
    }
}

@Suppress("UNCHECKED_CAST")
fun <R : Result> SeriesStatConfig<R>.windowed(
    durationMillis: Long,
    slices: Int = 10,
): SeriesStatConfig<R> = WindowedSeriesConfig(this, durationMillis, slices) as SeriesStatConfig<R>

@Suppress("UNCHECKED_CAST")
fun <R : Result> PairedStatConfig<R>.windowed(
    durationMillis: Long,
    slices: Int = 10,
): PairedStatConfig<R> = WindowedPairedConfig(this, durationMillis, slices) as PairedStatConfig<R>

@Suppress("UNCHECKED_CAST")
fun <R : Result> VectorStatConfig<R>.windowed(
    durationMillis: Long,
    slices: Int = 10,
): VectorStatConfig<R> = WindowedVectorConfig(this, durationMillis, slices) as VectorStatConfig<R>

@Suppress("UNCHECKED_CAST")
fun <R : Result> DiscreteStatConfig<R>.windowed(
    durationMillis: Long,
    slices: Int = 10,
): DiscreteStatConfig<R> = WindowedDiscreteConfig(this, durationMillis, slices) as DiscreteStatConfig<R>

// ========== Vectorized (Series template replicated per dimension) ==========

@Serializable @SerialName("Vectorized")
data class VectorizedStatConfig(val dimensions: Int, val template: StatConfig) :
    VectorStatConfig<ResultList<Result>> {
    override fun materialize(concurrency: Concurrency): VectorStat<ResultList<Result>> {
        @Suppress("UNCHECKED_CAST")
        val tpl = requireSeries(template, "Vectorized").materialize(concurrency) as SeriesStat<Result>
        return VectorizedStat(dimensions, tpl, concurrency)
    }
}

@Suppress("UNCHECKED_CAST")
fun <R : Result> SeriesStatConfig<R>.vectorized(dimensions: Int): VectorStatConfig<ResultList<R>> =
    VectorizedStatConfig(dimensions, this) as VectorStatConfig<ResultList<R>>

// ========== Transform / Filter via expression AST ==========

/**
 * Apply [expr] as the value transform on every update — wire-friendly
 * counterpart of `SeriesStat<R>.transformValue { … }`. The Kotlin lambda is
 * built at materialize time and forwards every input through `expr.eval`;
 * the AST itself ([ScalarExpr]) is what travels on the wire.
 */
@Serializable @SerialName("TransformValueSeries")
data class TransformValueSeriesConfig(val inner: StatConfig, val expr: ScalarExpr) : SeriesStatConfig<Result> {
    override fun materialize(concurrency: Concurrency): SeriesStat<Result> {
        @Suppress("UNCHECKED_CAST")
        val materialized = requireSeries(inner, "TransformValueSeries").materialize(concurrency) as SeriesStat<Result>
        return materialized.transformValue { expr.eval(it) }
    }
}

@Serializable @SerialName("TransformValueDiscrete")
data class TransformValueDiscreteConfig(val inner: StatConfig, val expr: ScalarExpr) : DiscreteStatConfig<Result> {
    override fun materialize(concurrency: Concurrency): DiscreteStat<Result> {
        @Suppress("UNCHECKED_CAST")
        val materialized = requireDiscrete(inner, "TransformValueDiscrete").materialize(concurrency) as DiscreteStat<Result>
        return materialized.transformValue { expr.eval(it.toDouble()).toLong() }
    }
}

@Serializable @SerialName("FilterValueSeries")
data class FilterValueSeriesConfig(val inner: StatConfig, val pred: BoolExpr) : SeriesStatConfig<Result> {
    override fun materialize(concurrency: Concurrency): SeriesStat<Result> {
        @Suppress("UNCHECKED_CAST")
        val materialized = requireSeries(inner, "FilterValueSeries").materialize(concurrency) as SeriesStat<Result>
        return materialized.filter { pred.eval(it) }
    }
}

@Serializable @SerialName("FilterValueDiscrete")
data class FilterValueDiscreteConfig(val inner: StatConfig, val pred: BoolExpr) : DiscreteStatConfig<Result> {
    override fun materialize(concurrency: Concurrency): DiscreteStat<Result> {
        @Suppress("UNCHECKED_CAST")
        val materialized = requireDiscrete(inner, "FilterValueDiscrete").materialize(concurrency) as DiscreteStat<Result>
        return materialized.filter { pred.eval(it.toDouble()) }
    }
}

@Suppress("UNCHECKED_CAST")
fun <R : Result> SeriesStatConfig<R>.transform(expr: ScalarExpr): SeriesStatConfig<R> =
    TransformValueSeriesConfig(this, expr) as SeriesStatConfig<R>

@Suppress("UNCHECKED_CAST")
fun <R : Result> DiscreteStatConfig<R>.transform(expr: ScalarExpr): DiscreteStatConfig<R> =
    TransformValueDiscreteConfig(this, expr) as DiscreteStatConfig<R>

@Suppress("UNCHECKED_CAST")
fun <R : Result> SeriesStatConfig<R>.filter(pred: BoolExpr): SeriesStatConfig<R> =
    FilterValueSeriesConfig(this, pred) as SeriesStatConfig<R>

@Suppress("UNCHECKED_CAST")
fun <R : Result> DiscreteStatConfig<R>.filter(pred: BoolExpr): DiscreteStatConfig<R> =
    FilterValueDiscreteConfig(this, pred) as DiscreteStatConfig<R>
