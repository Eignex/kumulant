package com.eignex.kumulant.operation

import com.eignex.koblas.F64VectorView
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.stream.DEFAULT_WINDOW_SLICES
import com.eignex.kumulant.stream.SliceRing
import kotlin.time.Duration

/**
 * Wrap a [SeriesStat] in a tumbling-slice sliding window of length [duration].
 *
 * Values are bucketed across [slices] ring-buffer slots; reads merge the in-window
 * slots using the underlying stat's [Stat.merge]. More [slices] smooths the boundary
 * at the cost of memory and merge work per read.
 */
internal fun <R : Result> SeriesStat<R>.windowed(
    duration: Duration,
    slices: Int = DEFAULT_WINDOW_SLICES,
    concurrency: Concurrency = Concurrency.None,
): SeriesStat<R> = if (this is WindowsInside<*>) {
    // Wrap the operator's delegate, not the operator: see WindowsInside for why a slice rotation
    // would otherwise restart the operator's state, and BandSeriesStat for the merge-side reason.
    @Suppress("UNCHECKED_CAST")
    (this as WindowsInside<R>).windowedInside(duration, slices, concurrency)
} else {
    WindowedSeriesStat(duration, slices, this, concurrency)
}

/** Paired-stat counterpart of [SeriesStat.windowed]. */
internal fun <R : Result> PairedStat<R>.windowed(
    duration: Duration,
    slices: Int = DEFAULT_WINDOW_SLICES,
    concurrency: Concurrency = Concurrency.None,
): PairedStat<R> = WindowedPairedStat(duration, slices, this, concurrency)

/** Vector-stat counterpart of [SeriesStat.windowed]. */
internal fun <R : Result> VectorStat<R>.windowed(
    duration: Duration,
    slices: Int = DEFAULT_WINDOW_SLICES,
    concurrency: Concurrency = Concurrency.None,
): VectorStat<R> = WindowedVectorStat(duration, slices, this, concurrency)

/** Discrete-stat counterpart of [SeriesStat.windowed]. */
internal fun <R : Result> DiscreteStat<R>.windowed(
    duration: Duration,
    slices: Int = DEFAULT_WINDOW_SLICES,
    concurrency: Concurrency = Concurrency.None,
): DiscreteStat<R> = WindowedDiscreteStat(duration, slices, this, concurrency)

/**
 * Build a fresh single-threaded accumulator from [template], merge in every active
 * slot at [timestampNanos], and read the result. Shared by the four [windowed]
 * adapters since their `read` is modality-agnostic.
 */
private fun <R : Result, S : Stat<R>> windowedRead(template: S, ring: SliceRing<R, S>, timestampNanos: Long): R {
    val acc = template.create(concurrency = Concurrency.None)
    ring.forEachActive(timestampNanos) { acc.merge(it.read(timestampNanos)) }
    return acc.read(timestampNanos)
}

internal class WindowedSeriesStat<R : Result>(
    private val windowDuration: Duration,
    private val slices: Int,
    template: SeriesStat<R>,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<R> {

    private val template = template.create(concurrency = this.concurrency)
    private val ring = SliceRing<R, SeriesStat<R>>(windowDuration, slices, concurrency) { c -> this.template.create(c) }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        ring.slotFor(timestampNanos)?.update(value, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): SeriesStat<R> =
        WindowedSeriesStat(windowDuration, slices, template, concurrency ?: this.concurrency)

    override fun read(timestampNanos: Long): R = windowedRead(template, ring, timestampNanos)
    override fun merge(values: R) = ring.mergeNow(values)
    override fun reset() = ring.reset()
}

internal class WindowedPairedStat<R : Result>(
    private val windowDuration: Duration,
    private val slices: Int,
    template: PairedStat<R>,
    override val concurrency: Concurrency = Concurrency.None,
) : PairedStat<R> {

    private val template = template.create(concurrency = this.concurrency)
    private val ring = SliceRing<R, PairedStat<R>>(windowDuration, slices, concurrency) { c -> this.template.create(c) }

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        ring.slotFor(timestampNanos)?.update(x, y, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): PairedStat<R> =
        WindowedPairedStat(windowDuration, slices, template, concurrency ?: this.concurrency)

    override fun read(timestampNanos: Long): R = windowedRead(template, ring, timestampNanos)
    override fun merge(values: R) = ring.mergeNow(values)
    override fun reset() = ring.reset()
}

internal class WindowedDiscreteStat<R : Result>(
    private val windowDuration: Duration,
    private val slices: Int,
    template: DiscreteStat<R>,
    override val concurrency: Concurrency = Concurrency.None,
) : DiscreteStat<R> {

    private val template = template.create(concurrency = this.concurrency)
    private val ring = SliceRing<R, DiscreteStat<R>>(
        windowDuration,
        slices,
        concurrency,
    ) { c -> this.template.create(c) }

    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        ring.slotFor(timestampNanos)?.update(value, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): DiscreteStat<R> =
        WindowedDiscreteStat(windowDuration, slices, template, concurrency ?: this.concurrency)

    override fun read(timestampNanos: Long): R = windowedRead(template, ring, timestampNanos)
    override fun merge(values: R) = ring.mergeNow(values)
    override fun reset() = ring.reset()
}

internal class WindowedVectorStat<R : Result>(
    private val windowDuration: Duration,
    private val slices: Int,
    template: VectorStat<R>,
    override val concurrency: Concurrency = Concurrency.None,
) : VectorStat<R> {

    private val template = template.create(concurrency = this.concurrency)
    private val ring = SliceRing<R, VectorStat<R>>(windowDuration, slices, concurrency) { c -> this.template.create(c) }

    override fun update(vector: F64VectorView, timestampNanos: Long, weight: Double) {
        ring.slotFor(timestampNanos)?.update(vector, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): VectorStat<R> =
        WindowedVectorStat(windowDuration, slices, template, concurrency ?: this.concurrency)

    override fun read(timestampNanos: Long): R = windowedRead(template, ring, timestampNanos)
    override fun merge(values: R) = ring.mergeNow(values)
    override fun reset() = ring.reset()
}
