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
 * Filter adapters. Constructed by the spec layer's
 * `filter(BoolExpr)` materialization in
 * `com.eignex.kumulant.schema.Operations.kt`: the predicate closure is built
 * from the AST at materialize time.
 */

internal class FilterSeriesStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val predicate: (Double) -> Boolean,
) : SeriesStat<R>,
    Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (predicate(value)) delegate.update(value, timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): SeriesStat<R> =
        FilterSeriesStat(delegate.create(concurrency), predicate)
}

internal class FilterPairedStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val predicate: (Double, Double) -> Boolean,
) : PairedStat<R>,
    Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        if (predicate(x, y)) delegate.update(x, y, timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): PairedStat<R> =
        FilterPairedStat(delegate.create(concurrency), predicate)
}

internal class FilterVectorStat<R : Result>(
    private val delegate: VectorStat<R>,
    private val predicate: (DoubleArray) -> Boolean,
) : VectorStat<R>,
    Stat<R> by delegate {
    override fun update(vector: VectorView, timestampNanos: Long, weight: Double) {
        if (predicate(vector.toDoubleArray())) delegate.update(vector, timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): VectorStat<R> =
        FilterVectorStat(delegate.create(concurrency), predicate)
}

internal class FilterDiscreteStat<R : Result>(
    private val delegate: DiscreteStat<R>,
    private val predicate: (Long) -> Boolean,
) : DiscreteStat<R>,
    Stat<R> by delegate {
    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        if (predicate(value)) delegate.update(value, timestampNanos, weight)
    }
    override fun create(concurrency: Concurrency?): DiscreteStat<R> =
        FilterDiscreteStat(delegate.create(concurrency), predicate)
}
