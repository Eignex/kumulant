package com.eignex.kumulant.operation

import com.eignex.kumulant.core.*
import com.eignex.kumulant.stream.NANOS_PER_SECOND
import kotlin.time.Duration

// Event timestamps are converted to seconds.
internal fun <R : Result> PairedStat<R>.withTimeAsX(): SeriesStat<R> =
    SeriesToPairedStat(this, x = { _, timestampNanos -> timestampNanos / NANOS_PER_SECOND }, y = { value, _ -> value })

// Event timestamps are converted to seconds.
internal fun <R : Result> PairedStat<R>.withTimeAsY(): SeriesStat<R> =
    SeriesToPairedStat(this, x = { value, _ -> value }, y = { _, timestampNanos -> timestampNanos / NANOS_PER_SECOND })

internal fun <R : Result> PairedStat<R>.withFixedX(fixedX: Double): SeriesStat<R> =
    SeriesToPairedStat(this, x = { _, _ -> fixedX }, y = { value, _ -> value })

internal fun <R : Result> PairedStat<R>.withFixedY(fixedY: Double): SeriesStat<R> =
    SeriesToPairedStat(this, x = { value, _ -> value }, y = { _, _ -> fixedY })

// Binds a series value to a paired delegate; x and y map (value, timestampNanos) to each axis.
internal class SeriesToPairedStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val x: (Double, Long) -> Double,
    private val y: (Double, Long) -> Double,
) : SeriesStat<R>,
    WindowsInside<R, SeriesStat<R>>,
    Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x(value, timestampNanos), y(value, timestampNanos), timestampNanos, weight)
    }

    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): SeriesStat<R> =
        SeriesToPairedStat(delegate.windowed(duration, slices, concurrency), x, y)

    override fun create(concurrency: Concurrency?): SeriesStat<R> = SeriesToPairedStat(delegate.create(concurrency), x, y)
}
