package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasCenterScale
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.schema.BandResult
import kotlin.time.Duration

// band derives the center / scale / lower / upper bounds from any inner stat whose
// result implements [HasCenterScale]. The wrapper forwards update/reset/create to the
// delegate; read projects to [BandResult]. Merge through the wrapper is intentionally
// unsupported - merge the inner stat directly when combining replicas.

/** Wrap this series stat to expose a `[lower, upper]` band of width [k] * scale around center. */
internal fun <R> SeriesStat<R>.band(k: Double): SeriesStat<BandResult>
    where R : HasCenterScale =
    BandSeriesStat(this, k)

internal class BandSeriesStat<R>(private val delegate: SeriesStat<R>, private val k: Double) :
    WindowsInside<BandResult>,
    SeriesStat<BandResult> where R : HasCenterScale {

    override val concurrency: Concurrency get() = delegate.concurrency

    override fun update(value: Double, timestampNanos: Long, weight: Double) =
        delegate.update(value, timestampNanos, weight)

    override fun read(timestampNanos: Long): BandResult {
        val r = delegate.read(timestampNanos)
        val c = r.center
        val s = r.scale
        return BandResult(center = c, scale = s, k = k, lower = c - k * s, upper = c + k * s)
    }

    override fun merge(values: BandResult): Unit =
        error("band wrapper cannot merge BandResult; merge the inner stat directly")

    /**
     * Band *around* a windowed inner stat rather than a window around a band.
     *
     * The two orders mean the same thing, since this wrapper only projects in [read], but only this
     * one works: a window reads by merging its slices into a fresh template, and merging *through*
     * this wrapper throws.
     */
    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): SeriesStat<BandResult> =
        BandSeriesStat(delegate.windowed(duration, slices, concurrency), k)

    override fun reset() = delegate.reset()

    override fun create(concurrency: Concurrency?): SeriesStat<BandResult> =
        BandSeriesStat(delegate.create(concurrency), k)
}
