package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.stream.monotonicMode

// withSelfLag lifts a PairedStat<R> into a SeriesStat<R> by self-pairing each input
// with the value seen k updates ago. The first k updates only warm the ring buffer
// and forward nothing; from update k+1 onward the inner paired stat sees (current, lag-k).
//
// Use case: lag-k autocorrelation is Pearson correlation of the (current, lag-k) pair,
// so CovarianceStat.withSelfLag(k) is the streaming autocorrelation primitive.
//
// Concurrency: per-cell atomics with bounded drift (category 1). The ring slot may briefly
// observe an out-of-order write under contention but the paired stat always sees some
// (current, past) pair the stream actually emitted.

/**
 * Lift a paired stat into a series stat by self-pairing each input with the value seen
 * [k] updates ago. The first [k] updates only warm the ring and forward nothing.
 *
 * The inner paired stat receives `(current, lag-k)` so a covariance / correlation stat
 * naturally computes lag-k autocovariance / autocorrelation.
 */
internal fun <R : Result> PairedStat<R>.withSelfLag(k: Int): SeriesStat<R> = WithSelfLagSeriesStat(this, k)

internal class WithSelfLagSeriesStat<R : Result>(private val delegate: PairedStat<R>, private val k: Int) :
    SeriesStat<R>,
    Stat<R> by delegate {

    init {
        require(k >= 1) { "withSelfLag k must be >= 1, got $k" }
    }

    private val mode = delegate.concurrency.monotonicMode()
    private val tick = mode.newLong(0L)
    private val ring = mode.newDoubleArray(k)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        val n = tick.addAndGet(1L)
        val slot = ((n - 1L) % k).toInt()
        val past = ring.load(slot)
        ring.store(slot, value)
        if (n > k) delegate.update(x = value, y = past, timestampNanos = timestampNanos, weight = weight)
    }

    override fun reset() {
        delegate.reset()
        tick.store(0L)
        for (i in 0 until k) ring.store(i, 0.0)
    }

    override fun create(concurrency: Concurrency?): SeriesStat<R> =
        WithSelfLagSeriesStat(delegate.create(concurrency), k)
}
