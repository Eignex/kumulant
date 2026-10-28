package com.eignex.kumulant.operation

import com.eignex.kumulant.core.*
import com.eignex.kumulant.stream.NANOS_PER_SECOND
import kotlin.time.Duration

// Event timestamps are converted to seconds.
internal fun <R : Result> PairedStat<R>.withTimeAsX(): SeriesStat<R> = WithTimeAsXStat(this)

// Event timestamps are converted to seconds.
internal fun <R : Result> PairedStat<R>.withTimeAsY(): SeriesStat<R> = WithTimeAsYStat(this)

internal fun <R : Result> PairedStat<R>.withFixedX(fixedX: Double): SeriesStat<R> = WithFixedXStat(this, fixedX)

internal fun <R : Result> PairedStat<R>.withFixedY(fixedY: Double): SeriesStat<R> = WithFixedYStat(this, fixedY)

internal class WithTimeAsXStat<R : Result>(private val delegate: PairedStat<R>) :
    SeriesStat<R>,
    WindowsInside<R, SeriesStat<R>>,
    Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(
            x = timestampNanos / NANOS_PER_SECOND,
            y = value,
            timestampNanos,
            weight,
        )
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): SeriesStat<R> =
        WithTimeAsXStat(delegate.windowed(duration, slices, concurrency))

    override fun create(concurrency: Concurrency?): SeriesStat<R> = WithTimeAsXStat(delegate.create(concurrency))
}

internal class WithTimeAsYStat<R : Result>(private val delegate: PairedStat<R>) :
    SeriesStat<R>,
    WindowsInside<R, SeriesStat<R>>,
    Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(
            x = value,
            y = timestampNanos / NANOS_PER_SECOND,
            timestampNanos,
            weight,
        )
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): SeriesStat<R> =
        WithTimeAsYStat(delegate.windowed(duration, slices, concurrency))

    override fun create(concurrency: Concurrency?): SeriesStat<R> = WithTimeAsYStat(delegate.create(concurrency))
}

internal class WithFixedXStat<R : Result>(private val delegate: PairedStat<R>, private val fixedX: Double) :
    SeriesStat<R>,
    WindowsInside<R, SeriesStat<R>>,
    Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x = fixedX, y = value, timestampNanos, weight)
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): SeriesStat<R> =
        WithFixedXStat(delegate.windowed(duration, slices, concurrency), fixedX)

    override fun create(concurrency: Concurrency?): SeriesStat<R> = WithFixedXStat(delegate.create(concurrency), fixedX)
}

internal class WithFixedYStat<R : Result>(private val delegate: PairedStat<R>, private val fixedY: Double) :
    SeriesStat<R>,
    WindowsInside<R, SeriesStat<R>>,
    Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x = value, y = fixedY, timestampNanos, weight)
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): SeriesStat<R> =
        WithFixedYStat(delegate.windowed(duration, slices, concurrency), fixedY)

    override fun create(concurrency: Concurrency?): SeriesStat<R> = WithFixedYStat(delegate.create(concurrency), fixedY)
}
