package com.eignex.kumulant.operation

import com.eignex.kumulant.core.*

/** Lift a paired stat into a series stat that feeds its x from the event timestamp (seconds). */
internal fun <R : Result> PairedStat<R>.withTimeAsX(): SeriesStat<R> = WithTimeAsXStat(this)

/** Lift a paired stat into a series stat that feeds its y from the event timestamp (seconds). */
internal fun <R : Result> PairedStat<R>.withTimeAsY(): SeriesStat<R> = WithTimeAsYStat(this)

/** Lift a paired stat into a series stat that always feeds [fixedX] as the x coordinate. */
internal fun <R : Result> PairedStat<R>.withFixedX(fixedX: Double): SeriesStat<R> = WithFixedXStat(this, fixedX)

/** Lift a paired stat into a series stat that always feeds [fixedY] as the y coordinate. */
internal fun <R : Result> PairedStat<R>.withFixedY(fixedY: Double): SeriesStat<R> = WithFixedYStat(this, fixedY)

/** Series-adapter implementation for [withTimeAsX]. */
internal class WithTimeAsXStat<R : Result>(private val delegate: PairedStat<R>) :
    SeriesStat<R>,
    Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(
            x = timestampNanos / 1e9,
            y = value,
            timestampNanos,
            weight,
        )
    }

    override fun create(concurrency: Concurrency?): SeriesStat<R> = WithTimeAsXStat(delegate.create(concurrency))
}

/** Series-adapter implementation for [withTimeAsY]. */
internal class WithTimeAsYStat<R : Result>(private val delegate: PairedStat<R>) :
    SeriesStat<R>,
    Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(
            x = value,
            y = timestampNanos / 1e9,
            timestampNanos,
            weight,
        )
    }

    override fun create(concurrency: Concurrency?): SeriesStat<R> = WithTimeAsYStat(delegate.create(concurrency))
}

/** Series-adapter implementation for [withFixedX]. */
internal class WithFixedXStat<R : Result>(private val delegate: PairedStat<R>, private val fixedX: Double) :
    SeriesStat<R>,
    Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x = fixedX, y = value, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): SeriesStat<R> = WithFixedXStat(delegate.create(concurrency), fixedX)
}

/** Series-adapter implementation for [withFixedY]. */
internal class WithFixedYStat<R : Result>(private val delegate: PairedStat<R>, private val fixedY: Double) :
    SeriesStat<R>,
    Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x = value, y = fixedY, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): SeriesStat<R> = WithFixedYStat(delegate.create(concurrency), fixedY)
}
