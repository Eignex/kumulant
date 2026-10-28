package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import kotlin.time.Duration

// The first k updates warm the ring. Inert weights preserve its history.
// Tick allocation, slot reads, and writes share a lock so pairs always contain initialized values.

// The delegate receives (current, lag-k), giving lag-k autocovariance or autocorrelation.
internal fun <R : Result> PairedStat<R>.withSelfLag(k: Int): SeriesStat<R> = WithSelfLagSeriesStat(this, k)

internal class WithSelfLagSeriesStat<R : Result>(private val delegate: PairedStat<R>, private val k: Int) :
    SeriesStat<R>,
    WindowsInside<R, SeriesStat<R>>,
    Stat<R> by delegate {

    init {
        require(k >= 1) { "withSelfLag k must be >= 1, got $k" }
    }

    private val history = LagBuffer(k, delegate.concurrency)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        history.update(value, weight) { past ->
            delegate.update(x = value, y = past, timestampNanos = timestampNanos, weight = weight)
        }
    }

    override fun reset() = history.reset { delegate.reset() }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): SeriesStat<R> =
        WithSelfLagSeriesStat(delegate.windowed(duration, slices, concurrency), k)

    override fun create(concurrency: Concurrency?): SeriesStat<R> =
        WithSelfLagSeriesStat(delegate.create(concurrency), k)
}
