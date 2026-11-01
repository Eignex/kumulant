package com.eignex.kumulant.operation

import com.eignex.koblas.Vector
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import kotlin.time.Duration

// The projections must round-trip so merged snapshots reconstruct the delegate result.
internal fun <R1 : Result, R2 : Result> PairedStat<R1>.mapResult(
    forward: (R1) -> R2,
    reverse: (R2) -> R1,
): PairedStat<R2> = MapResultPairedStat(this, forward, reverse)

private class MappedResultCore<R1 : Result, R2 : Result>(
    private val delegate: Stat<R1>,
    private val forward: (R1) -> R2,
    private val reverse: (R2) -> R1,
) : Stat<R2> {
    override val concurrency: Concurrency get() = delegate.concurrency
    override fun validateMerge(values: R2) = delegate.validateMerge(reverse(values))
    override fun merge(values: R2, timestampNanos: Long) = delegate.merge(reverse(values), timestampNanos)
    override fun reset() = delegate.reset()
    override fun read(timestampNanos: Long): R2 = forward(delegate.read(timestampNanos))
    override fun create(concurrency: Concurrency?): Stat<R2> =
        error("MappedResultCore.create is not used; the modality adapter rebuilds itself")
}

internal class MapResultPairedStat<R1 : Result, R2 : Result>(
    private val delegate: PairedStat<R1>,
    private val forward: (R1) -> R2,
    private val reverse: (R2) -> R1,
) : PairedStat<R2>,
    WindowsInside<R2, PairedStat<R2>>,
    Stat<R2> by MappedResultCore(delegate, forward, reverse) {
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) =
        delegate.update(x, y, timestampNanos, weight)
    override fun windowedInside(duration: Duration, slices: Int, concurrency: Concurrency): PairedStat<R2> =
        MapResultPairedStat(delegate.windowed(duration, slices, concurrency), forward, reverse)

    override fun create(concurrency: Concurrency?): PairedStat<R2> =
        MapResultPairedStat(delegate.create(concurrency), forward, reverse)
}
