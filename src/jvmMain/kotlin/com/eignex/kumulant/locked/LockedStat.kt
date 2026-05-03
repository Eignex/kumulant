package com.eignex.kumulant.locked

import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.stream.StreamMode
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * Wrap a [SeriesStat] so that all updates, reads, merges, and resets are serialized by a
 * [java.util.concurrent.locks.ReentrantReadWriteLock]. Slower than [com.eignex.kumulant.stream.AtomicMode] under
 * contention, but works with any stat regardless of internal concurrency support.
 */
fun <R : Result> SeriesStat<R>.locked(): SeriesStat<R> = LockedSeriesStat(this)

/** Paired-stat counterpart of [SeriesStat.locked]. */
fun <R : Result> PairedStat<R>.locked(): PairedStat<R> = LockedPairedStat(this)

/** Vector-stat counterpart of [SeriesStat.locked]. */
fun <R : Result> VectorStat<R>.locked(): VectorStat<R> = LockedVectorStat(this)

/** Discrete-stat counterpart of [SeriesStat.locked]. */
fun <R : Result> DiscreteStat<R>.locked(): DiscreteStat<R> = LockedDiscreteStat(this)

/**
 * Shared lock + locked merge/read/reset that the four [locked] adapters delegate to.
 * `update` is left to the subclass since its signature varies per modality; `create`
 * is also subclass-specific so the modality return type is preserved.
 */
private class RWLockedCore<R : Result>(
    private val delegate: Stat<R>,
) : Stat<R> {
    val lock = ReentrantReadWriteLock()
    override val mode: StreamMode get() = delegate.mode
    override fun merge(values: R) = lock.write { delegate.merge(values) }
    override fun reset() = lock.write { delegate.reset() }
    override fun read(timestampNanos: Long): R = lock.read { delegate.read(timestampNanos) }
    override fun create(mode: StreamMode?): Stat<R> =
        error("RWLockedCore.create is not used; the modality adapter rebuilds itself")
}

/** [SeriesStat] wrapper that serializes access through a read/write lock. */
internal class LockedSeriesStat<R : Result> private constructor(
    private val delegate: SeriesStat<R>,
    private val core: RWLockedCore<R>,
) : SeriesStat<R>, Stat<R> by core {
    constructor(delegate: SeriesStat<R>) : this(delegate, RWLockedCore(delegate))
    override fun update(value: Double, timestampNanos: Long, weight: Double) =
        core.lock.write { delegate.update(value, timestampNanos, weight) }
    override fun create(mode: StreamMode?): SeriesStat<R> =
        LockedSeriesStat(delegate.create(mode))
}

/** [PairedStat] wrapper that serializes access through a read/write lock. */
internal class LockedPairedStat<R : Result> private constructor(
    private val delegate: PairedStat<R>,
    private val core: RWLockedCore<R>,
) : PairedStat<R>, Stat<R> by core {
    constructor(delegate: PairedStat<R>) : this(delegate, RWLockedCore(delegate))
    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) =
        core.lock.write { delegate.update(x, y, timestampNanos, weight) }
    override fun create(mode: StreamMode?): PairedStat<R> =
        LockedPairedStat(delegate.create(mode))
}

/** [VectorStat] wrapper that serializes access through a read/write lock. */
internal class LockedVectorStat<R : Result> private constructor(
    private val delegate: VectorStat<R>,
    private val core: RWLockedCore<R>,
) : VectorStat<R>, Stat<R> by core {
    constructor(delegate: VectorStat<R>) : this(delegate, RWLockedCore(delegate))
    override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) =
        core.lock.write { delegate.update(vector, timestampNanos, weight) }
    override fun create(mode: StreamMode?): VectorStat<R> =
        LockedVectorStat(delegate.create(mode))
}

/** [DiscreteStat] wrapper that serializes access through a read/write lock. */
internal class LockedDiscreteStat<R : Result> private constructor(
    private val delegate: DiscreteStat<R>,
    private val core: RWLockedCore<R>,
) : DiscreteStat<R>, Stat<R> by core {
    constructor(delegate: DiscreteStat<R>) : this(delegate, RWLockedCore(delegate))
    override fun update(value: Long, timestampNanos: Long, weight: Double) =
        core.lock.write { delegate.update(value, timestampNanos, weight) }
    override fun create(mode: StreamMode?): DiscreteStat<R> =
        LockedDiscreteStat(delegate.create(mode))
}
