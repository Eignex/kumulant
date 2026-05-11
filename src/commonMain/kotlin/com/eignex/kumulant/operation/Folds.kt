package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat

/**
 * Fold adapters. Constructed by the spec layer's `foldPaired(ScalarExpr)` /
 * `foldVector(ScalarExpr)` materialization in
 * [com.eignex.kumulant.schema.Operations.kt].
 */

internal class FoldVectorStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val transform: (DoubleArray) -> Double
) : VectorStat<R>, Stat<R> by delegate {
    override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) {
        delegate.update(transform(vector), timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): VectorStat<R> =
        FoldVectorStat(delegate.create(concurrency), transform)
}

internal class FoldPairedStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val transform: (Double, Double) -> Double
) : PairedStat<R>, Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        delegate.update(transform(x, y), timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): PairedStat<R> =
        FoldPairedStat(delegate.create(concurrency), transform)
}
