package com.eignex.kumulant.operation

import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.core.*

fun <R : Result> PairedStat<R>.withTimeAsX(): SeriesStat<R> = WithTimeAsXStat(this)
fun <R : Result> PairedStat<R>.withTimeAsY(): SeriesStat<R> = WithTimeAsYStat(this)
fun <R : Result> PairedStat<R>.withFixedX(fixedX: Double): SeriesStat<R> = WithFixedXStat(this, fixedX)
fun <R : Result> PairedStat<R>.withFixedY(fixedY: Double): SeriesStat<R> = WithFixedYStat(this, fixedY)

class WithTimeAsXStat<R : Result>(
    private val delegate: PairedStat<R>
) : SeriesStat<R>, Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(
            x = timestampNanos / 1e9,
            y = value,
            timestampNanos,
            weight
        )
    }

    override fun create(mode: StreamMode?): SeriesStat<R> {
        return WithTimeAsXStat(delegate.create(mode))
    }
}

class WithTimeAsYStat<R : Result>(
    private val delegate: PairedStat<R>
) : SeriesStat<R>, Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(
            x = value,
            y = timestampNanos / 1e9,
            timestampNanos,
            weight
        )
    }

    override fun create(mode: StreamMode?): SeriesStat<R> {
        return WithTimeAsYStat(delegate.create(mode))
    }
}

class WithFixedXStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val fixedX: Double
) : SeriesStat<R>, Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x = fixedX, y = value, timestampNanos, weight)
    }

    override fun create(mode: StreamMode?): SeriesStat<R> {
        return WithFixedXStat(delegate.create(mode), fixedX)
    }
}

class WithFixedYStat<R : Result>(
    private val delegate: PairedStat<R>,
    private val fixedY: Double
) : SeriesStat<R>, Stat<R> by delegate {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x = value, y = fixedY, timestampNanos, weight)
    }

    override fun create(mode: StreamMode?): SeriesStat<R> {
        return WithFixedYStat(delegate.create(mode), fixedY)
    }
}
