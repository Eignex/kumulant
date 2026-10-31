package com.eignex.kumulant.operation

import com.eignex.koblas.Vector
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import kotlin.time.Duration

internal class TransformValueStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val transform: (Double) -> Double,
) : SeriesStat<R>,
    WindowsInside<R, SeriesStat<R>>,
    Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(transform(value), timestampNanos, weight)
    }
    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): SeriesStat<R> =
        TransformValueStat(delegate.windowed(duration, slices, concurrency), transform)

    override fun create(concurrency: Concurrency?): SeriesStat<R> =
        TransformValueStat(delegate.create(concurrency), transform)
}

internal class TransformPairStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val transform: (Double, Double) -> Pair<Double, Double>,
) : PairedStat<R>,
    WindowsInside<R, PairedStat<R>>,
    Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        val (mx, my) = transform(x, y)
        delegate.update(mx, my, timestampNanos, weight)
    }
    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): PairedStat<R> =
        TransformPairStat(delegate.windowed(duration, slices, concurrency), transform)

    override fun create(concurrency: Concurrency?): PairedStat<R> =
        TransformPairStat(delegate.create(concurrency), transform)
}

internal class TransformVectorStat<R : Result>(
    private val delegate: VectorStat<R>,
    private val transform: (DoubleArray) -> DoubleArray = { it },
    private val vectorTransform: ((Vector) -> DoubleArray)? = null,
) : VectorStat<R>,
    WindowsInside<R, VectorStat<R>>,
    Stat<R> by delegate {
    override fun update(vector: Vector, timestampNanos: Long, weight: Double) {
        delegate.update(vectorTransform?.invoke(vector) ?: transform(vector.toDoubleArray()), timestampNanos, weight)
    }
    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): VectorStat<R> =
        TransformVectorStat(delegate.windowed(duration, slices, concurrency), transform, vectorTransform)

    override fun create(concurrency: Concurrency?): VectorStat<R> =
        TransformVectorStat(delegate.create(concurrency), transform, vectorTransform)

    companion object {
        fun <R : Result> vector(delegate: VectorStat<R>, transform: (Vector) -> DoubleArray): TransformVectorStat<R> =
            TransformVectorStat(delegate, vectorTransform = transform)
    }
}

internal class TransformLongStat<R : Result>(
    private val delegate: DiscreteStat<R>,
    private val transform: (Long) -> Long,
) : DiscreteStat<R>,
    WindowsInside<R, DiscreteStat<R>>,
    Stat<R> by delegate {
    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        delegate.update(transform(value), timestampNanos, weight)
    }
    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): DiscreteStat<R> =
        TransformLongStat(delegate.windowed(duration, slices, concurrency), transform)

    override fun create(concurrency: Concurrency?): DiscreteStat<R> =
        TransformLongStat(delegate.create(concurrency), transform)
}

internal fun <R : Result> SeriesStat<R>.withValue(value: Double): SeriesStat<R> = TransformValueStat(this) { value }

internal fun <R : Result> DiscreteStat<R>.withValue(value: Long): DiscreteStat<R> = ConstantValueDiscreteStat(
    this,
    value,
)

internal class ConstantValueDiscreteStat<R : Result>(private val delegate: DiscreteStat<R>, private val value: Long) :
    DiscreteStat<R>,
    WindowsInside<R, DiscreteStat<R>>,
    Stat<R> by delegate {
    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        delegate.update(this.value, timestampNanos, weight)
    }
    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): DiscreteStat<R> =
        ConstantValueDiscreteStat(delegate.windowed(duration, slices, concurrency), value)

    override fun create(concurrency: Concurrency?): DiscreteStat<R> =
        ConstantValueDiscreteStat(delegate.create(concurrency), value)
}

internal fun <R : Result> DiscreteStat<R>.asSeries(): SeriesStat<R> = DiscreteAsSeriesStat(this)

internal fun <R : Result> SeriesStat<R>.asDiscrete(): DiscreteStat<R> = SeriesAsDiscreteStat(this)

internal class DiscreteAsSeriesStat<R : Result>(private val delegate: DiscreteStat<R>) :
    SeriesStat<R>,
    WindowsInside<R, SeriesStat<R>>,
    Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(value.toLong(), timestampNanos, weight)
    }
    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): SeriesStat<R> =
        DiscreteAsSeriesStat(delegate.windowed(duration, slices, concurrency))

    override fun create(concurrency: Concurrency?): SeriesStat<R> = DiscreteAsSeriesStat(delegate.create(concurrency))
}

internal class SeriesAsDiscreteStat<R : Result>(private val delegate: SeriesStat<R>) :
    DiscreteStat<R>,
    WindowsInside<R, DiscreteStat<R>>,
    Stat<R> by delegate {
    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        delegate.update(value.toDouble(), timestampNanos, weight)
    }
    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): DiscreteStat<R> =
        SeriesAsDiscreteStat(delegate.windowed(duration, slices, concurrency))

    override fun create(concurrency: Concurrency?): DiscreteStat<R> = SeriesAsDiscreteStat(delegate.create(concurrency))
}
