package com.eignex.kumulant.operation

import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat


class MapFromVectorStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val transform: VectorTransform
) : VectorStat<R>, Stat<R> by delegate {
    override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) {
        delegate.update(transform.apply(vector), timestampNanos, weight)
    }
    override fun create(mode: StreamMode?): VectorStat<R> {
        return MapFromVectorStat(delegate.create(mode), transform)
    }
}

class MapFromPairedStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val transform: PairedTransform
) : PairedStat<R>, Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        delegate.update(transform.apply(x, y), timestampNanos, weight)
    }
    override fun create(mode: StreamMode?): PairedStat<R> {
        return MapFromPairedStat(delegate.create(mode), transform)
    }
}

class MapSeriesStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val transform: DoubleTransform
) : SeriesStat<R>, Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(transform.apply(value), timestampNanos, weight)
    }
    override fun create(mode: StreamMode?): SeriesStat<R> {
        return MapSeriesStat(delegate.create(mode), transform)
    }
}

class OnXStat<R : Result>(
    private val delegate: SeriesStat<R>
) : PairedStat<R>, Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x, timestampNanos, weight)
    }
    override fun create(mode: StreamMode?): PairedStat<R> {
        return OnXStat(delegate.create(mode))
    }
}

class OnYStat<R : Result>(
    private val delegate: SeriesStat<R>
) : PairedStat<R>, Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        delegate.update(y, timestampNanos, weight)
    }
    override fun create(mode: StreamMode?): PairedStat<R> {
        return OnYStat(delegate.create(mode))
    }
}

class AtIndexStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val index: Int
) : VectorStat<R>, Stat<R> by delegate {
    override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) {
        delegate.update(vector[index], timestampNanos, weight)
    }
    override fun create(mode: StreamMode?): VectorStat<R> {
        return AtIndexStat(delegate.create(mode), index)
    }
}

class AtIndicesStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val indexX: Int,
    private val indexY: Int
) : VectorStat<R>, Stat<R> by delegate {
    override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) {
        delegate.update(vector[indexX], vector[indexY], timestampNanos, weight)
    }
    override fun create(mode: StreamMode?): VectorStat<R> {
        return AtIndicesStat(delegate.create(mode), indexX, indexY)
    }
}

class WithTimeAsXStat<R : Result>(
    private val delegate: PairedStat<R>
) : SeriesStat<R>, Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x = timestampNanos / 1e9, y = value, timestampNanos, weight)
    }
    override fun create(mode: StreamMode?): SeriesStat<R> {
        return WithTimeAsXStat(delegate.create(mode))
    }
}

class WithTimeAsYStat<R : Result>(
    private val delegate: PairedStat<R>
) : SeriesStat<R>, Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x = value, y = timestampNanos / 1e9, timestampNanos, weight)
    }
    override fun create(mode: StreamMode?): SeriesStat<R> {
        return WithTimeAsYStat(delegate.create(mode))
    }
}

class WithFixedXStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val fixedX: Double
) : SeriesStat<R>, Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x = fixedX, y = value, timestampNanos, weight)
    }
    override fun create(mode: StreamMode?): SeriesStat<R> {
        return WithFixedXStat(delegate.create(mode), fixedX)
    }
}

class WithFixedYStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val fixedY: Double
) : SeriesStat<R>, Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x = value, y = fixedY, timestampNanos, weight)
    }
    override fun create(mode: StreamMode?): SeriesStat<R> {
        return WithFixedYStat(delegate.create(mode), fixedY)
    }
}

class WithWeightStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val weight: Double
) : SeriesStat<R>, Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(value, timestampNanos, this.weight)
    }
    override fun create(mode: StreamMode?): SeriesStat<R> {
        return WithWeightStat(delegate.create(mode), weight)
    }
}

fun <R : Result> SeriesStat<R>.mapFromVector(transform: VectorTransform): VectorStat<R> = MapFromVectorStat(
    this,
    transform
)
fun <R : Result> SeriesStat<R>.mapFromPaired(transform: PairedTransform): PairedStat<R> = MapFromPairedStat(
    this,
    transform
)
fun <R : Result> SeriesStat<R>.mapSeries(transform: DoubleTransform): SeriesStat<R> = MapSeriesStat(this, transform)

fun <R : Result> SeriesStat<R>.onX(): PairedStat<R> = OnXStat(this)
fun <R : Result> SeriesStat<R>.onY(): PairedStat<R> = OnYStat(this)

fun <R : Result> SeriesStat<R>.atIndex(index: Int): VectorStat<R> = AtIndexStat(this, index)
fun <R : Result> PairedStat<R>.atIndices(indexX: Int, indexY: Int): VectorStat<R> = AtIndicesStat(this, indexX, indexY)

fun <R : Result> PairedStat<R>.withTimeAsX(): SeriesStat<R> = WithTimeAsXStat(this)
fun <R : Result> PairedStat<R>.withTimeAsY(): SeriesStat<R> = WithTimeAsYStat(this)
fun <R : Result> PairedStat<R>.withFixedX(fixedX: Double): SeriesStat<R> = WithFixedXStat(this, fixedX)
fun <R : Result> PairedStat<R>.withFixedY(fixedY: Double): SeriesStat<R> = WithFixedYStat(this, fixedY)

fun <R : Result> SeriesStat<R>.withWeight(weight: Double): SeriesStat<R> = WithWeightStat(this, weight)

fun <R : Result> SeriesStat<R>.withValue(value: Double): SeriesStat<R> = mapSeries { value }
