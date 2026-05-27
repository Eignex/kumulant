package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.stream.firstWriterMode
import com.eignex.kumulant.stream.monotonicMode

// Pre-update shift adapters: forward a value derived from the stream's recent history to the
// delegate. Each operator keeps a tiny per-instance ring (or single-cell) buffer of past values
// and emits the lagged value, the k-th difference, or the time-derivative.
//
// Until enough history accumulates the operators suppress forwarding entirely: the first k
// (for lag/diff) or first single (for derivative) updates are absorbed silently while the
// buffer warms up.
//
// Concurrency: per-cell atomics with bounded drift (category 1). Concurrent updates may briefly
// observe an out-of-order ring slot but never throw; the forwarded value is always some value
// the stream actually emitted.
//
// The wire counterparts live in `schema/Operations.kt` (DiffSeries, LagSeries, DerivativeSeries).

/** Forward the value seen [k] updates ago. The first [k] updates warm the ring and forward nothing. */
internal fun <R : Result> SeriesStat<R>.lag(k: Int): SeriesStat<R> = LagSeriesStat(this, k)

/** Forward the k-th difference `value - value[t - k]`. The first [k] updates warm the ring and forward nothing. */
internal fun <R : Result> SeriesStat<R>.diff(k: Int = 1): SeriesStat<R> = DiffSeriesStat(this, k)

/**
 * Forward the time derivative `(value - prev) / (timestampNanos - prevTimestampNanos)` expressed in
 * units-per-second. The first update warms the cell and forwards nothing. Coincident timestamps
 * are dropped (would otherwise produce infinity).
 */
internal fun <R : Result> SeriesStat<R>.derivative(): SeriesStat<R> = DerivativeSeriesStat(this)

private fun requireK(k: Int) = require(k >= 1) { "shift k must be >= 1, got $k" }

internal class LagSeriesStat<R : Result>(private val delegate: SeriesStat<R>, private val k: Int) :
    SeriesStat<R>,
    Stat<R> by delegate {
    init {
        requireK(k)
    }

    private val mode = delegate.concurrency.monotonicMode()
    private val tick = mode.newLong(0L)
    private val ring = mode.newDoubleArray(k)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        val n = tick.addAndGet(1L)
        val slot = ((n - 1L) % k).toInt()
        val past = ring.load(slot)
        ring.store(slot, value)
        if (n > k) delegate.update(past, timestampNanos, weight)
    }

    override fun reset() {
        delegate.reset()
        tick.store(0L)
        for (i in 0 until k) ring.store(i, 0.0)
    }

    override fun create(concurrency: Concurrency?): SeriesStat<R> = LagSeriesStat(delegate.create(concurrency), k)
}

internal class DiffSeriesStat<R : Result>(private val delegate: SeriesStat<R>, private val k: Int) :
    SeriesStat<R>,
    Stat<R> by delegate {
    init {
        requireK(k)
    }

    private val mode = delegate.concurrency.monotonicMode()
    private val tick = mode.newLong(0L)
    private val ring = mode.newDoubleArray(k)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        val n = tick.addAndGet(1L)
        val slot = ((n - 1L) % k).toInt()
        val past = ring.load(slot)
        ring.store(slot, value)
        if (n > k) delegate.update(value - past, timestampNanos, weight)
    }

    override fun reset() {
        delegate.reset()
        tick.store(0L)
        for (i in 0 until k) ring.store(i, 0.0)
    }

    override fun create(concurrency: Concurrency?): SeriesStat<R> = DiffSeriesStat(delegate.create(concurrency), k)
}

internal class DerivativeSeriesStat<R : Result>(private val delegate: SeriesStat<R>) :
    SeriesStat<R>,
    Stat<R> by delegate {

    private val mode = delegate.concurrency.monotonicMode()
    private val tsMode = delegate.concurrency.firstWriterMode()
    private val initialized = mode.newLong(0L)
    private val lastValue = mode.newDouble(0.0)
    private val lastTimestamp = tsMode.newLong(0L)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        val seen = initialized.addAndGet(1L)
        val prevValue = lastValue.load()
        val prevTs = lastTimestamp.load()
        lastValue.store(value)
        lastTimestamp.store(timestampNanos)
        if (seen <= 1L) return
        val deltaNanos = timestampNanos - prevTs
        if (deltaNanos == 0L) return
        val rate = (value - prevValue) * NANOS_PER_SECOND / deltaNanos
        delegate.update(rate, timestampNanos, weight)
    }

    override fun reset() {
        delegate.reset()
        initialized.store(0L)
        lastValue.store(0.0)
        lastTimestamp.store(0L)
    }

    override fun create(concurrency: Concurrency?): SeriesStat<R> = DerivativeSeriesStat(delegate.create(concurrency))

    companion object {
        private const val NANOS_PER_SECOND: Double = 1_000_000_000.0
    }
}
