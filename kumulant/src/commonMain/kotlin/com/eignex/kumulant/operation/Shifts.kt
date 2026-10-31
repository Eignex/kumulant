package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.isNotPositiveWeight
import com.eignex.kumulant.stream.NANOS_PER_SECOND
import com.eignex.kumulant.stream.firstWriterMode
import com.eignex.kumulant.stream.monotonicMode
import kotlin.time.Duration

// Lag and diff warm the ring for k updates; derivative needs one previous sample.
// Inert weights preserve the stored history.
// Lag and diff serialize tick allocation, slot reads, and writes to keep history initialized.
// Derivative uses per-cell atomics and drops non-positive time deltas.

// The first k updates warm the ring before forwarding begins.
internal fun <R : Result> SeriesStat<R>.lag(k: Int): SeriesStat<R> = LagSeriesStat(this, k)

// The first k updates warm the ring before forwarding begins.
internal fun <R : Result> SeriesStat<R>.diff(k: Int = 1): SeriesStat<R> = DiffSeriesStat(this, k)

// Time differences are converted from nanoseconds to seconds; coincident timestamps are dropped.
internal fun <R : Result> SeriesStat<R>.derivative(): SeriesStat<R> = DerivativeSeriesStat(this)

private fun requireK(k: Int) = require(k >= 1) { "shift k must be >= 1, got $k" }

internal class LagSeriesStat<R : Result>(private val delegate: SeriesStat<R>, private val k: Int) :
    SeriesStat<R>,
    WindowsInside<R, SeriesStat<R>>,
    Stat<R> by delegate {
    init {
        requireK(k)
    }

    private val history = LagBuffer(k, delegate.concurrency)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        history.update(value, weight) { past ->
            delegate.update(past, timestampNanos, weight)
        }
    }

    override fun reset() = history.reset { delegate.reset() }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): SeriesStat<R> =
        LagSeriesStat(delegate.windowed(duration, slices, concurrency), k)

    override fun create(concurrency: Concurrency?): SeriesStat<R> = LagSeriesStat(delegate.create(concurrency), k)
}

internal class DiffSeriesStat<R : Result>(private val delegate: SeriesStat<R>, private val k: Int) :
    SeriesStat<R>,
    WindowsInside<R, SeriesStat<R>>,
    Stat<R> by delegate {
    init {
        requireK(k)
    }

    private val history = LagBuffer(k, delegate.concurrency)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        history.update(value, weight) { past ->
            delegate.update(value - past, timestampNanos, weight)
        }
    }

    override fun reset() = history.reset { delegate.reset() }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): SeriesStat<R> =
        DiffSeriesStat(delegate.windowed(duration, slices, concurrency), k)

    override fun create(concurrency: Concurrency?): SeriesStat<R> = DiffSeriesStat(delegate.create(concurrency), k)
}

internal class DerivativeSeriesStat<R : Result>(private val delegate: SeriesStat<R>) :
    SeriesStat<R>,
    WindowsInside<R, SeriesStat<R>>,
    Stat<R> by delegate {

    private val mode = delegate.concurrency.monotonicMode()
    private val tsMode = delegate.concurrency.firstWriterMode()
    private val initialized = mode.newLong(0L)
    private val lastValue = mode.newDouble(0.0)
    private val lastTimestamp = tsMode.newLong(0L)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        // Non-positive rather than merely inert: the reference point has no inverse, so a downdate
        // would replace it and forward a rate as an ordinary observation. See Stat.
        if (weight.isNotPositiveWeight()) return
        val seen = initialized.addAndGet(1L)
        val prevValue = lastValue.load()
        val prevTs = lastTimestamp.load()
        val deltaNanos = timestampNanos - prevTs
        // Stored unless the stamp is older than the reference: at the same instant the newer value
        // is the series value there, but an out-of-order arrival must not drag the reference back.
        if (seen <= 1L || deltaNanos >= 0L) {
            lastValue.store(value)
            lastTimestamp.store(timestampNanos)
        }
        if (seen <= 1L) return
        // Not just `== 0L`: a negative delta divides through to a rate of the wrong sign.
        if (deltaNanos <= 0L) return
        val rate = (value - prevValue) * NANOS_PER_SECOND / deltaNanos
        delegate.update(rate, timestampNanos, weight)
    }

    override fun reset() {
        delegate.reset()
        initialized.store(0L)
        lastValue.store(0.0)
        lastTimestamp.store(0L)
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): SeriesStat<R> =
        DerivativeSeriesStat(delegate.windowed(duration, slices, concurrency))

    override fun create(concurrency: Concurrency?): SeriesStat<R> = DerivativeSeriesStat(delegate.create(concurrency))
}
