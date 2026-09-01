package com.eignex.kumulant.operation

import com.eignex.koblas.core.F64VectorLike
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat

/**
 * Fold adapters. Constructed by the spec layer's `foldPaired(ScalarExpr)` /
 * `foldVector(ScalarExpr)` materialization in
 * `com.eignex.kumulant.schema.Operations.kt`.
 */

internal class FoldVectorStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val transform: (DoubleArray) -> Double = { 0.0 },
    private val vectorTransform: ((F64VectorLike) -> Double)? = null,
) : VectorStat<R>,
    Stat<R> by delegate {
    override fun update(vector: F64VectorLike, timestampNanos: Long, weight: Double) {
        delegate.update(vectorTransform?.invoke(vector) ?: transform(vector.toDoubleArray()), timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): VectorStat<R> =
        FoldVectorStat(delegate.create(concurrency), transform, vectorTransform)

    companion object {
        fun <R : Result> vector(delegate: SeriesStat<R>, transform: (F64VectorLike) -> Double): FoldVectorStat<R> =
            FoldVectorStat(delegate, vectorTransform = transform)
    }
}

internal class FoldPairedStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val transform: (Double, Double) -> Double,
) : PairedStat<R>,
    Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        delegate.update(transform(x, y), timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): PairedStat<R> =
        FoldPairedStat(delegate.create(concurrency), transform)
}

internal class FoldVectorPairedStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val foldX: (DoubleArray) -> Double = { 0.0 },
    private val foldY: (DoubleArray) -> Double = { 0.0 },
    private val vectorFoldX: ((F64VectorLike) -> Double)? = null,
    private val vectorFoldY: ((F64VectorLike) -> Double)? = null,
) : VectorStat<R>,
    Stat<R> by delegate {
    override fun update(vector: F64VectorLike, timestampNanos: Long, weight: Double) {
        if (vectorFoldX != null && vectorFoldY != null) {
            delegate.update(vectorFoldX(vector), vectorFoldY(vector), timestampNanos, weight)
        } else {
            val materialized = vector.toDoubleArray()
            delegate.update(foldX(materialized), foldY(materialized), timestampNanos, weight)
        }
    }
    override fun create(concurrency: Concurrency?): VectorStat<R> =
        FoldVectorPairedStat(delegate.create(concurrency), foldX, foldY, vectorFoldX, vectorFoldY)

    companion object {
        fun <R : Result> vector(
            delegate: PairedStat<R>,
            foldX: (F64VectorLike) -> Double,
            foldY: (F64VectorLike) -> Double,
        ): FoldVectorPairedStat<R> = FoldVectorPairedStat(delegate, vectorFoldX = foldX, vectorFoldY = foldY)
    }
}
