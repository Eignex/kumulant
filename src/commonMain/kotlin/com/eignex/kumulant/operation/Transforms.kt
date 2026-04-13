package com.eignex.kumulant.operation

import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat

fun interface ValueTransform {
    fun apply(value: Double): Double
}

fun interface PairTransform {
    fun apply(x: Double, y: Double): Pair<Double, Double>
}

fun interface VectorTransform {
    fun apply(vector: DoubleArray): DoubleArray
}

class TransformValueStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val transform: ValueTransform
) : SeriesStat<R>, Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(transform.apply(value), timestampNanos, weight)
    }

    override fun create(mode: StreamMode?): SeriesStat<R> {
        return TransformValueStat(delegate.create(mode), transform)
    }
}

class TransformPairStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val transform: PairTransform
) : PairedStat<R>, Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        val (mappedX, mappedY) = transform.apply(x, y)
        delegate.update(mappedX, mappedY, timestampNanos, weight)
    }

    override fun create(mode: StreamMode?): PairedStat<R> {
        return TransformPairStat(delegate.create(mode), transform)
    }
}

class TransformVectorStat<R : Result>(
    private val delegate: VectorStat<R>,
    private val transform: VectorTransform
) : VectorStat<R>, Stat<R> by delegate {
    override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) {
        delegate.update(transform.apply(vector), timestampNanos, weight)
    }

    override fun create(mode: StreamMode?): VectorStat<R> {
        return TransformVectorStat(delegate.create(mode), transform)
    }
}

fun <R : Result> SeriesStat<R>.transformValue(transform: ValueTransform): SeriesStat<R> = TransformValueStat(
    this,
    transform
)

fun <R : Result> SeriesStat<R>.withValue(value: Double): SeriesStat<R> = transformValue { value }

fun <R : Result> PairedStat<R>.transformPair(transform: PairTransform): PairedStat<R> = TransformPairStat(
    this,
    transform
)

fun <R : Result> PairedStat<R>.transformX(transform: ValueTransform): PairedStat<R> = transformPair { x, y ->
    transform.apply(x) to y
}

fun <R : Result> PairedStat<R>.transformY(transform: ValueTransform): PairedStat<R> = transformPair { x, y ->
    x to transform.apply(y)
}

fun <R : Result> VectorStat<R>.transformVector(transform: VectorTransform): VectorStat<R> = TransformVectorStat(
    this,
    transform
)
