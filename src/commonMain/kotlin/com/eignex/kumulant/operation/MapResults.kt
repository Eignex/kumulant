package com.eignex.kumulant.operation

import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.VectorStat

/**
 * Present a [SeriesStat]'s result as a different type via an invertible projection.
 *
 * [forward] produces the exposed result on read; [reverse] reconstructs the underlying
 * result on merge. Both must round-trip for merge semantics to hold.
 */
fun <R1 : Result, R2 : Result> SeriesStat<R1>.mapResult(
    forward: (R1) -> R2,
    reverse: (R2) -> R1
): SeriesStat<R2> = MapResultSeriesStat(this, forward, reverse)

/** Paired-stat counterpart of [SeriesStat.mapResult]. */
fun <R1 : Result, R2 : Result> PairedStat<R1>.mapResult(
    forward: (R1) -> R2,
    reverse: (R2) -> R1
): PairedStat<R2> = MapResultPairedStat(this, forward, reverse)

/** Vector-stat counterpart of [SeriesStat.mapResult]. */
fun <R1 : Result, R2 : Result> VectorStat<R1>.mapResult(
    forward: (R1) -> R2,
    reverse: (R2) -> R1
): VectorStat<R2> = MapResultVectorStat(this, forward, reverse)

/** Adapter implementing the series-stat variant of [mapResult]. */
class MapResultSeriesStat<R1 : Result, R2 : Result>(
    private val delegate: SeriesStat<R1>,
    private val forward: (R1) -> R2,
    private val reverse: (R2) -> R1
) : SeriesStat<R2> {
    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        delegate.update(value, timestampNanos, weight)
    }

    override fun merge(values: R2) {
        delegate.merge(reverse(values))
    }

    override fun reset() {
        delegate.reset()
    }

    override fun read(timestampNanos: Long): R2 {
        return forward(delegate.read(timestampNanos))
    }

    override fun create(mode: StreamMode?): SeriesStat<R2> {
        return MapResultSeriesStat(delegate.create(mode), forward, reverse)
    }
}

/** Adapter implementing the paired-stat variant of [mapResult]. */
class MapResultPairedStat<R1 : Result, R2 : Result>(
    private val delegate: PairedStat<R1>,
    private val forward: (R1) -> R2,
    private val reverse: (R2) -> R1
) : PairedStat<R2> {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        delegate.update(x, y, timestampNanos, weight)
    }

    override fun merge(values: R2) {
        delegate.merge(reverse(values))
    }

    override fun reset() {
        delegate.reset()
    }

    override fun read(timestampNanos: Long): R2 {
        return forward(delegate.read(timestampNanos))
    }

    override fun create(mode: StreamMode?): PairedStat<R2> {
        return MapResultPairedStat(delegate.create(mode), forward, reverse)
    }
}

/** Adapter implementing the vector-stat variant of [mapResult]. */
class MapResultVectorStat<R1 : Result, R2 : Result>(
    private val delegate: VectorStat<R1>,
    private val forward: (R1) -> R2,
    private val reverse: (R2) -> R1
) : VectorStat<R2> {
    override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) {
        delegate.update(vector, timestampNanos, weight)
    }

    override fun merge(values: R2) {
        delegate.merge(reverse(values))
    }

    override fun reset() {
        delegate.reset()
    }

    override fun read(timestampNanos: Long): R2 {
        return forward(delegate.read(timestampNanos))
    }

    override fun create(mode: StreamMode?): VectorStat<R2> {
        return MapResultVectorStat(delegate.create(mode), forward, reverse)
    }
}
