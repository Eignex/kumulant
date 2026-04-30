package com.eignex.kumulant.operation

import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.stream.SerialMode
import com.eignex.kumulant.stream.SliceRing
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.defaultStreamMode
import kotlin.time.Duration

/**
 * Wrap a [SeriesStat] in a tumbling-slice sliding window of length [duration].
 *
 * Values are bucketed across [slices] ring-buffer slots; reads merge the in-window
 * slots using the underlying stat's [Stat.merge]. More [slices] smooths the boundary
 * at the cost of memory and merge work per read.
 */
fun <R : Result> SeriesStat<R>.windowed(
    duration: Duration,
    slices: Int = 10,
    mode: StreamMode = defaultStreamMode
): SeriesStat<R> = WindowedSeriesStat(duration, slices, this, mode)

/** Paired-stat counterpart of [SeriesStat.windowed]. */
fun <R : Result> PairedStat<R>.windowed(
    duration: Duration,
    slices: Int = 10,
    mode: StreamMode = defaultStreamMode
): PairedStat<R> = WindowedPairedStat(duration, slices, this, mode)

/** Vector-stat counterpart of [SeriesStat.windowed]. */
fun <R : Result> VectorStat<R>.windowed(
    duration: Duration,
    slices: Int = 10,
    mode: StreamMode = defaultStreamMode
): VectorStat<R> = WindowedVectorStat(duration, slices, this, mode)

/** Discrete-stat counterpart of [SeriesStat.windowed]. */
fun <R : Result> DiscreteStat<R>.windowed(
    duration: Duration,
    slices: Int = 10,
    mode: StreamMode = defaultStreamMode
): DiscreteStat<R> = WindowedDiscreteStat(duration, slices, this, mode)

/**
 * Build a fresh single-threaded accumulator from [template], merge in every active
 * slot at [timestampNanos], and read the result. Shared by the four [windowed]
 * adapters since their `read` is modality-agnostic.
 */
private fun <R : Result, S : Stat<R>> windowedRead(
    template: S,
    ring: SliceRing<R, S>,
    timestampNanos: Long,
): R {
    val acc = template.create(mode = SerialMode)
    ring.forEachActive(timestampNanos) { acc.merge(it.read(timestampNanos)) }
    return acc.read(timestampNanos)
}

/** Implementation of [SeriesStat.windowed]; see that function for behavior. */
internal class WindowedSeriesStat<R : Result>(
    private val windowDuration: Duration,
    private val slices: Int,
    template: SeriesStat<R>,
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<R> {

    private val template = template.create(mode = this.mode)
    private val ring = SliceRing<R, SeriesStat<R>>(windowDuration, slices, mode) { m -> this.template.create(m) }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        ring.slotFor(timestampNanos)?.update(value, timestampNanos, weight)
    }

    override fun create(mode: StreamMode?): SeriesStat<R> =
        WindowedSeriesStat(windowDuration, slices, template, mode ?: this.mode)

    override fun read(timestampNanos: Long): R = windowedRead(template, ring, timestampNanos)
    override fun merge(values: R) = ring.mergeNow(values)
    override fun reset() = ring.reset()
}

/** Implementation of [PairedStat.windowed]; see that function for behavior. */
internal class WindowedPairedStat<R : Result>(
    private val windowDuration: Duration,
    private val slices: Int,
    template: PairedStat<R>,
    val mode: StreamMode = defaultStreamMode,
) : PairedStat<R> {

    private val template = template.create(mode = this.mode)
    private val ring = SliceRing<R, PairedStat<R>>(windowDuration, slices, mode) { m -> this.template.create(m) }

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        ring.slotFor(timestampNanos)?.update(x, y, timestampNanos, weight)
    }

    override fun create(mode: StreamMode?): PairedStat<R> =
        WindowedPairedStat(windowDuration, slices, template, mode ?: this.mode)

    override fun read(timestampNanos: Long): R = windowedRead(template, ring, timestampNanos)
    override fun merge(values: R) = ring.mergeNow(values)
    override fun reset() = ring.reset()
}

/** Implementation of [DiscreteStat.windowed]; see that function for behavior. */
internal class WindowedDiscreteStat<R : Result>(
    private val windowDuration: Duration,
    private val slices: Int,
    template: DiscreteStat<R>,
    val mode: StreamMode = defaultStreamMode,
) : DiscreteStat<R> {

    private val template = template.create(mode = this.mode)
    private val ring = SliceRing<R, DiscreteStat<R>>(windowDuration, slices, mode) { m -> this.template.create(m) }

    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        ring.slotFor(timestampNanos)?.update(value, timestampNanos, weight)
    }

    override fun create(mode: StreamMode?): DiscreteStat<R> =
        WindowedDiscreteStat(windowDuration, slices, template, mode ?: this.mode)

    override fun read(timestampNanos: Long): R = windowedRead(template, ring, timestampNanos)
    override fun merge(values: R) = ring.mergeNow(values)
    override fun reset() = ring.reset()
}

/** Implementation of [VectorStat.windowed]; see that function for behavior. */
internal class WindowedVectorStat<R : Result>(
    private val windowDuration: Duration,
    private val slices: Int,
    template: VectorStat<R>,
    val mode: StreamMode = defaultStreamMode,
) : VectorStat<R> {

    private val template = template.create(mode = this.mode)
    private val ring = SliceRing<R, VectorStat<R>>(windowDuration, slices, mode) { m -> this.template.create(m) }

    override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) {
        ring.slotFor(timestampNanos)?.update(vector, timestampNanos, weight)
    }

    override fun create(mode: StreamMode?): VectorStat<R> =
        WindowedVectorStat(windowDuration, slices, template, mode ?: this.mode)

    override fun read(timestampNanos: Long): R = windowedRead(template, ring, timestampNanos)
    override fun merge(values: R) = ring.mergeNow(values)
    override fun reset() = ring.reset()
}
