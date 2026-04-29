package com.eignex.kumulant.operation

import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.core.*

/** Adapt a [SeriesStat] to accept vector input by folding each vector to a scalar via [transform]. */
fun <R : Result> SeriesStat<R>.foldVector(transform: VectorFold): VectorStat<R> =
    FoldVectorStat(
        this,
        transform
    )

/** Adapt a [SeriesStat] to accept paired input by folding each (x, y) to a scalar via [transform]. */
fun <R : Result> SeriesStat<R>.foldPaired(transform: PairFold): PairedStat<R> =
    FoldPairedStat(
        this,
        transform
    )

/** Reduces a vector to a single scalar. */
fun interface VectorFold {
    /** Return the scalar to forward downstream. */
    fun apply(vector: DoubleArray): Double
}

/** Reduces an (x, y) pair to a single scalar. */
fun interface PairFold {
    /** Return the scalar to forward downstream. */
    fun apply(x: Double, y: Double): Double
}

/** Adapter implementing [foldVector]. */
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

/** Adapter implementing [foldPaired]. */
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
