package com.eignex.kumulant.operation

import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat

fun <R : Result> SeriesStat<R>.filter(predicate: DoublePredicate): SeriesStat<R> = FilterSeriesStat(this, predicate)
fun <R : Result> PairedStat<R>.filter(predicate: PairedPredicate): PairedStat<R> = FilterPairedStat(this, predicate)
fun <R : Result> VectorStat<R>.filter(predicate: VectorPredicate): VectorStat<R> = FilterVectorStat(this, predicate)

fun interface DoublePredicate {
    fun test(value: Double): Boolean
}

fun interface PairedPredicate {
    fun test(x: Double, y: Double): Boolean
}

fun interface VectorPredicate {
    fun test(vector: DoubleArray): Boolean
}

class FilterSeriesStat<R : Result>(
    private val delegate: SeriesStat<R>,
    private val predicate: DoublePredicate
) : SeriesStat<R>, Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (predicate.test(value)) {
            delegate.update(value, timestampNanos, weight)
        }
    }
    override fun create(mode: StreamMode?): SeriesStat<R> {
        return FilterSeriesStat(delegate.create(mode), predicate)
    }
}

class FilterPairedStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val predicate: PairedPredicate
) : PairedStat<R>, Stat<R> by delegate {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        if (predicate.test(x, y)) {
            delegate.update(x, y, timestampNanos, weight)
        }
    }
    override fun create(mode: StreamMode?): PairedStat<R> {
        return FilterPairedStat(delegate.create(mode), predicate)
    }
}

class FilterVectorStat<R : Result>(
    private val delegate: VectorStat<R>,
    private val predicate: VectorPredicate
) : VectorStat<R>, Stat<R> by delegate {
    override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) {
        if (predicate.test(vector)) {
            delegate.update(vector, timestampNanos, weight)
        }
    }
    override fun create(mode: StreamMode?): VectorStat<R> {
        return FilterVectorStat(delegate.create(mode), predicate)
    }
}
