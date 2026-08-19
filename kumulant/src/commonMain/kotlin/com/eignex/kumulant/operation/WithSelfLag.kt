package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.monotonicMode
import com.eignex.kumulant.stream.serializedLock

// withSelfLag lifts a PairedStat<R> into a SeriesStat<R> by self-pairing each input
// with the value seen k updates ago. The first k updates only warm the ring buffer
// and forward nothing; from update k+1 onward the inner paired stat sees (current, lag-k).
//
// Use case: lag-k autocorrelation is Pearson correlation of the (current, lag-k) pair,
// so CovarianceStat.withSelfLag(k) is the streaming autocorrelation primitive.
//
// An inert weight is dropped before the ring is touched: the ring is what the lagged half of
// each pair is read back out of, so admitting an observation that carries no multiplicity would
// pair it with a real one later at that update's weight. See [Stat].
//
// Concurrency: the ring update is serialised behind the level's lock, which is a noop only under
// Concurrency.None. Claiming the tick, reading the ring slot and storing into it is one indivisible
// step: split apart, the paired stat receives (current, 0.0), a pair the stream never emitted, which
// is an unbounded error rather than the bounded drift the concurrent levels promise.

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
    private val lock = delegate.concurrency.serializedLock()
    private val tick = mode.newLong(0L)
    private val ring = mode.newDoubleArray(k)

    // Serialised: a torn read of the ring would pair the current value with the initial 0.0, which
    // the stream never emitted, and feed that pair into the delegate as a real observation.
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        lock.guarded {
            val n = tick.addAndGet(1L)
            val slot = ((n - 1L) % k).toInt()
            val past = ring.load(slot)
            ring.store(slot, value)
            if (n > k) delegate.update(x = value, y = past, timestampNanos = timestampNanos, weight = weight)
        }
    }

    override fun reset() {
        delegate.reset()
        tick.store(0L)
        for (i in 0 until k) ring.store(i, 0.0)
    }

    override fun create(concurrency: Concurrency?): SeriesStat<R> =
        WithSelfLagSeriesStat(delegate.create(concurrency), k)
}
