package com.eignex.kumulant.operation

import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.core.*

fun <R : Result> SeriesStat<R>.foldVector(transform: VectorFold): VectorStat<R> =
    FoldVectorStat(
        this,
        transform
    )

fun <R : Result> SeriesStat<R>.foldPaired(transform: PairFold): PairedStat<R> =
    FoldPairedStat(
        this,
        transform
    )

fun interface VectorFold {
    fun apply(vector: DoubleArray): Double
}

fun interface PairFold {
    fun apply(x: Double, y: Double): Double
}

class FoldVectorStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val transform: VectorFold
) : VectorStat<R>, Stat<R> by delegate {
    override fun update(
        vector: DoubleArray,
        timestampNanos: Long,
        weight: Double
    ) {
        delegate.update(transform.apply(vector), timestampNanos, weight)
    }

    override fun create(mode: StreamMode?): VectorStat<R> {
        return FoldVectorStat(delegate.create(mode), transform)
    }
}

class FoldPairedStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val transform: PairFold
) : PairedStat<R>, Stat<R> by delegate {
    override fun update(
        x: Double,
        y: Double,
        timestampNanos: Long,
        weight: Double
    ) {
        delegate.update(transform.apply(x, y), timestampNanos, weight)
    }

    override fun create(mode: StreamMode?): PairedStat<R> {
        return FoldPairedStat(delegate.create(mode), transform)
    }
}
