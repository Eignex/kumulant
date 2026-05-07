package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat

/** Scalar-to-scalar transform applied pre-update. */
fun interface DoubleTransform {
    /** Map [value] to the scalar forwarded downstream. */
    fun apply(value: Double): Double
}

/** (x, y)-to-(x', y') transform applied pre-update. */
fun interface PairTransform {
    /** Map the pair to the pair forwarded downstream. */
    fun apply(x: Double, y: Double): Pair<Double, Double>
}

/** Vector-to-vector transform applied pre-update. */
fun interface VectorTransform {
    /** Map [vector] to the vector forwarded downstream. */
    fun apply(vector: DoubleArray): DoubleArray
}

/** Long-to-Long transform applied pre-update. */
fun interface LongTransform {
    /** Map [value] to the Long forwarded downstream. */
    fun apply(value: Long): Long
}

/** Adapter implementing [SeriesStat.transformValue]. */
internal class TransformValueStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val transform: DoubleTransform
) : SeriesStat<R>, Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(transform.apply(value), timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): SeriesStat<R> {
        return TransformValueStat(delegate.create(concurrency), transform)
    }
}

/** Adapter implementing [PairedStat.transformPair]. */
internal class TransformPairStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val transform: PairTransform
) : PairedStat<R>, Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        val (mappedX, mappedY) = transform.apply(x, y)
        delegate.update(mappedX, mappedY, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): PairedStat<R> {
        return TransformPairStat(delegate.create(concurrency), transform)
    }
}

/** Adapter implementing [VectorStat.transformVector]. */
internal class TransformVectorStat<R : Result>(
    private val delegate: VectorStat<R>,
    private val transform: VectorTransform
) : VectorStat<R>, Stat<R> by delegate {
    override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) {
        delegate.update(transform.apply(vector), timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): VectorStat<R> {
        return TransformVectorStat(delegate.create(concurrency), transform)
    }
}

/** Adapter implementing [DiscreteStat.transformValue]. */
internal class TransformLongStat<R : Result>(
    private val delegate: DiscreteStat<R>,
    private val transform: LongTransform
) : DiscreteStat<R>, Stat<R> by delegate {
    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        delegate.update(transform.apply(value), timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): DiscreteStat<R> {
        return TransformLongStat(delegate.create(concurrency), transform)
    }
}

/** Apply [transform] to each incoming value before update. */
fun <R : Result> SeriesStat<R>.transformValue(transform: DoubleTransform): SeriesStat<R> = TransformValueStat(
    this,
    transform
)

/** Replace the incoming value with the constant [value] on every update. */
fun <R : Result> SeriesStat<R>.withValue(value: Double): SeriesStat<R> = ConstantValueStat(this, value)

/**
 * Wire-friendly counterpart of `transformValue { value }`: holds the constant
 * as a primitive instead of a captured lambda, so a config layer can record
 * and replay it.
 */
internal class ConstantValueStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val value: Double,
) : SeriesStat<R>, Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(this.value, timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): SeriesStat<R> =
        ConstantValueStat(delegate.create(concurrency), value)
}

/** Apply [transform] to each incoming (x, y) pair before update. */
fun <R : Result> PairedStat<R>.transformPair(transform: PairTransform): PairedStat<R> = TransformPairStat(
    this,
    transform
)

/** Apply [transform] to the x coordinate only. */
fun <R : Result> PairedStat<R>.transformX(transform: DoubleTransform): PairedStat<R> = transformPair { x, y ->
    transform.apply(x) to y
}

/** Apply [transform] to the y coordinate only. */
fun <R : Result> PairedStat<R>.transformY(transform: DoubleTransform): PairedStat<R> = transformPair { x, y ->
    x to transform.apply(y)
}

/** Apply [transform] to each incoming vector before update. */
fun <R : Result> VectorStat<R>.transformVector(transform: VectorTransform): VectorStat<R> = TransformVectorStat(
    this,
    transform
)

/** Apply [transform] to each incoming Long value before update. */
fun <R : Result> DiscreteStat<R>.transformValue(transform: LongTransform): DiscreteStat<R> = TransformLongStat(
    this,
    transform
)

/** Replace the incoming Long value with the constant [value] on every update. */
fun <R : Result> DiscreteStat<R>.withValue(value: Long): DiscreteStat<R> = ConstantValueDiscreteStat(this, value)

/** Discrete counterpart of [ConstantValueStat]. */
internal class ConstantValueDiscreteStat<R : Result>(
    private val delegate: DiscreteStat<R>,
    private val value: Long,
) : DiscreteStat<R>, Stat<R> by delegate {
    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        delegate.update(this.value, timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): DiscreteStat<R> =
        ConstantValueDiscreteStat(delegate.create(concurrency), value)
}

/**
 * View this [DiscreteStat] as a [SeriesStat] that accepts `Double`. Each Double input
 * is cast via [Double.toLong] (truncates toward zero) before being forwarded. Compose
 * with [atX], [atY], [atIndex] to drive a discrete stat from paired or vector streams.
 * Apply your own rounding upstream via [transformValue] if truncation is wrong for you.
 */
fun <R : Result> DiscreteStat<R>.asSeries(): SeriesStat<R> = DiscreteAsSeriesStat(this)

/** View this [SeriesStat] as a [DiscreteStat] that accepts `Long` (cast to Double via [Long.toDouble]). */
fun <R : Result> SeriesStat<R>.asDiscrete(): DiscreteStat<R> = SeriesAsDiscreteStat(this)

/** Adapter implementing [DiscreteStat.asSeries]. */
internal class DiscreteAsSeriesStat<R : Result>(
    private val delegate: DiscreteStat<R>
) : SeriesStat<R>, Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(value.toLong(), timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): SeriesStat<R> =
        DiscreteAsSeriesStat(delegate.create(concurrency))
}

/** Adapter implementing [SeriesStat.asDiscrete]. */
internal class SeriesAsDiscreteStat<R : Result>(
    private val delegate: SeriesStat<R>
) : DiscreteStat<R>, Stat<R> by delegate {
    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        delegate.update(value.toDouble(), timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): DiscreteStat<R> =
        SeriesAsDiscreteStat(delegate.create(concurrency))
}
