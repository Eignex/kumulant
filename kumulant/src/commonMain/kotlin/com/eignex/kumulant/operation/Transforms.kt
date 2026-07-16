package com.eignex.kumulant.operation

import com.eignex.koblas.VectorView
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat

/**
 * Pre-update transform adapters. The lambda-bound variants
 * ([TransformValueStat] / [TransformPairStat] / [TransformVectorStat] /
 * [TransformLongStat]) are constructed by the spec layer's
 * `transform(ScalarExpr)` / `transformPair(xExpr, yExpr)` /
 * `transformElement(ScalarExpr)` / `transformVector(VectorExpr)` materialization
 * in `com.eignex.kumulant.schema.Operations.kt`; the closure is built from the
 * AST at materialize time.
 *
 * The exceptions are [withValue], [asSeries], and [asDiscrete]: parameter-only
 * adapters with no lambda, exposed on the live surface for direct use.
 */

internal class TransformValueStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val transform: (Double) -> Double,
) : SeriesStat<R>,
    Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(transform(value), timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): SeriesStat<R> =
        TransformValueStat(delegate.create(concurrency), transform)
}

internal class TransformPairStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val transform: (Double, Double) -> Pair<Double, Double>,
) : PairedStat<R>,
    Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        val (mx, my) = transform(x, y)
        delegate.update(mx, my, timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): PairedStat<R> =
        TransformPairStat(delegate.create(concurrency), transform)
}

internal class TransformVectorStat<R : Result>(
    private val delegate: VectorStat<R>,
    private val transform: (DoubleArray) -> DoubleArray,
) : VectorStat<R>,
    Stat<R> by delegate {
    override fun update(vector: VectorView, timestampNanos: Long, weight: Double) {
        delegate.update(transform(vector.toDoubleArray()), timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): VectorStat<R> =
        TransformVectorStat(delegate.create(concurrency), transform)
}

internal class TransformLongStat<R : Result>(
    private val delegate: DiscreteStat<R>,
    private val transform: (Long) -> Long,
) : DiscreteStat<R>,
    Stat<R> by delegate {
    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        delegate.update(transform(value), timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): DiscreteStat<R> =
        TransformLongStat(delegate.create(concurrency), transform)
}

/** Replace every incoming Double with the constant [value]. */
internal fun <R : Result> SeriesStat<R>.withValue(value: Double): SeriesStat<R> = ConstantValueStat(this, value)

/** Replace every incoming Long with the constant [value]. */
internal fun <R : Result> DiscreteStat<R>.withValue(value: Long): DiscreteStat<R> = ConstantValueDiscreteStat(
    this,
    value,
)

internal class ConstantValueStat<R : Result>(private val delegate: SeriesStat<R>, private val value: Double) :
    SeriesStat<R>,
    Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(this.value, timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): SeriesStat<R> =
        ConstantValueStat(delegate.create(concurrency), value)
}

internal class ConstantValueDiscreteStat<R : Result>(private val delegate: DiscreteStat<R>, private val value: Long) :
    DiscreteStat<R>,
    Stat<R> by delegate {
    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        delegate.update(this.value, timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): DiscreteStat<R> =
        ConstantValueDiscreteStat(delegate.create(concurrency), value)
}

/**
 * View this [DiscreteStat] as a [SeriesStat] that accepts `Double`. Each Double input
 * is cast via [Double.toLong] (truncates toward zero) before being forwarded.
 */
internal fun <R : Result> DiscreteStat<R>.asSeries(): SeriesStat<R> = DiscreteAsSeriesStat(this)

/** View this [SeriesStat] as a [DiscreteStat] that accepts `Long` (cast to Double via [Long.toDouble]). */
internal fun <R : Result> SeriesStat<R>.asDiscrete(): DiscreteStat<R> = SeriesAsDiscreteStat(this)

internal class DiscreteAsSeriesStat<R : Result>(private val delegate: DiscreteStat<R>) :
    SeriesStat<R>,
    Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(value.toLong(), timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): SeriesStat<R> = DiscreteAsSeriesStat(delegate.create(concurrency))
}

internal class SeriesAsDiscreteStat<R : Result>(private val delegate: SeriesStat<R>) :
    DiscreteStat<R>,
    Stat<R> by delegate {
    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        delegate.update(value.toDouble(), timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): DiscreteStat<R> = SeriesAsDiscreteStat(delegate.create(concurrency))
}
