package com.eignex.kumulant.operation

import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat

/** Drop observations that fail [predicate] before forwarding to this stat. */
fun <R : Result> SeriesStat<R>.filter(predicate: DoublePredicate): SeriesStat<R> = FilterSeriesStat(this, predicate)

/** Drop paired observations that fail [predicate]. */
fun <R : Result> PairedStat<R>.filter(predicate: PairedPredicate): PairedStat<R> = FilterPairedStat(this, predicate)

/** Drop vector observations that fail [predicate]. */
fun <R : Result> VectorStat<R>.filter(predicate: VectorPredicate): VectorStat<R> = FilterVectorStat(this, predicate)

/** Drop discrete (Long) observations that fail [predicate]. */
fun <R : Result> DiscreteStat<R>.filter(predicate: LongPredicate): DiscreteStat<R> = FilterDiscreteStat(this, predicate)

/** Predicate on a single value. */
fun interface DoublePredicate {
    /** Return true to accept [value]. */
    fun test(value: Double): Boolean
}

/** Predicate on a paired (x, y) observation. */
fun interface PairedPredicate {
    /** Return true to accept the pair. */
    fun test(x: Double, y: Double): Boolean
}

/** Predicate on a vector observation. */
fun interface VectorPredicate {
    /** Return true to accept the vector. */
    fun test(vector: DoubleArray): Boolean
}

/** Predicate on a single Long value. */
fun interface LongPredicate {
    /** Return true to accept [value]. */
    fun test(value: Long): Boolean
}

/** Adapter that gates updates to a [SeriesStat] by a [DoublePredicate]. */
internal class FilterSeriesStat<R : Result>(
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

/** Adapter that gates updates to a [PairedStat] by a [PairedPredicate]. */
internal class FilterPairedStat<R : Result>(
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

/** Adapter that gates updates to a [VectorStat] by a [VectorPredicate]. */
internal class FilterVectorStat<R : Result>(
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

/** Adapter that gates updates to a [DiscreteStat] by a [LongPredicate]. */
internal class FilterDiscreteStat<R : Result>(
    private val delegate: DiscreteStat<R>,
    private val predicate: LongPredicate
) : DiscreteStat<R>, Stat<R> by delegate {
    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        if (predicate.test(value)) {
            delegate.update(value, timestampNanos, weight)
        }
    }
    override fun create(mode: StreamMode?): DiscreteStat<R> {
        return FilterDiscreteStat(delegate.create(mode), predicate)
    }
}
