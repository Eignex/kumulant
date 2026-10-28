package com.eignex.kumulant.operation

import com.eignex.koblas.Vector
import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.Stat
import com.eignex.kumulant.core.VectorStat
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.stream.DEFAULT_WINDOW_SLICES
import com.eignex.kumulant.stream.SliceRing
import kotlin.time.Duration

// Reads merge active slices; finer slices improve expiry granularity at a memory and merge cost.
internal fun <R : Result> SeriesStat<R>.windowed(
    duration: Duration,
    slices: Int = DEFAULT_WINDOW_SLICES,
    concurrency: Concurrency = Concurrency.None,
): SeriesStat<R> = windowedOutsideOrInside(duration, slices, concurrency) {
    WindowedSeriesStat(duration, slices, this, concurrency)
}

internal fun <R : Result> PairedStat<R>.windowed(
    duration: Duration,
    slices: Int = DEFAULT_WINDOW_SLICES,
    concurrency: Concurrency = Concurrency.None,
): PairedStat<R> = windowedOutsideOrInside(duration, slices, concurrency) {
    WindowedPairedStat(duration, slices, this, concurrency)
}

internal fun <R : Result> VectorStat<R>.windowed(
    duration: Duration,
    slices: Int = DEFAULT_WINDOW_SLICES,
    concurrency: Concurrency = Concurrency.None,
): VectorStat<R> = windowedOutsideOrInside(duration, slices, concurrency) {
    WindowedVectorStat(duration, slices, this, concurrency)
}

internal fun <R : Result> DiscreteStat<R>.windowed(
    duration: Duration,
    slices: Int = DEFAULT_WINDOW_SLICES,
    concurrency: Concurrency = Concurrency.None,
): DiscreteStat<R> = windowedOutsideOrInside(duration, slices, concurrency) {
    WindowedDiscreteStat(duration, slices, this, concurrency)
}

// Windowing the delegate preserves one operator state across slice rotations.
private inline fun <R : Result, S : Stat<R>> S.windowedOutsideOrInside(
    duration: Duration,
    slices: Int,
    concurrency: Concurrency,
    outside: () -> S,
): S = if (this is WindowsInside<*, *>) {
    @Suppress("UNCHECKED_CAST")
    (this as WindowsInside<R, S>).windowedInside(duration, slices, concurrency)
} else {
    outside()
}

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
        if (weight.isInertWeight()) return
        ring.slotFor(timestampNanos)?.update(value, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): SeriesStat<R> =
        WindowedSeriesStat(windowDuration, slices, template, concurrency ?: this.concurrency)

    override fun read(timestampNanos: Long): R = windowedRead(template, ring, timestampNanos)
    override fun validateMerge(values: R) = template.validateMerge(values)
    override fun merge(values: R) {
        validateMerge(values)
        ring.mergeNow(values)
    }
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
        if (weight.isInertWeight()) return
        ring.slotFor(timestampNanos)?.update(x, y, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): PairedStat<R> =
        WindowedPairedStat(windowDuration, slices, template, concurrency ?: this.concurrency)

    override fun read(timestampNanos: Long): R = windowedRead(template, ring, timestampNanos)
    override fun validateMerge(values: R) = template.validateMerge(values)
    override fun merge(values: R) {
        validateMerge(values)
        ring.mergeNow(values)
    }
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
        if (weight.isInertWeight()) return
        ring.slotFor(timestampNanos)?.update(value, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): DiscreteStat<R> =
        WindowedDiscreteStat(windowDuration, slices, template, concurrency ?: this.concurrency)

    override fun read(timestampNanos: Long): R = windowedRead(template, ring, timestampNanos)
    override fun validateMerge(values: R) = template.validateMerge(values)
    override fun merge(values: R) {
        validateMerge(values)
        ring.mergeNow(values)
    }
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

    override fun update(vector: Vector, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        ring.slotFor(timestampNanos)?.update(vector, timestampNanos, weight)
    }

    override fun create(concurrency: Concurrency?): VectorStat<R> =
        WindowedVectorStat(windowDuration, slices, template, concurrency ?: this.concurrency)

    override fun read(timestampNanos: Long): R = windowedRead(template, ring, timestampNanos)
    override fun validateMerge(values: R) = template.validateMerge(values)
    override fun merge(values: R) {
        validateMerge(values)
        ring.mergeNow(values)
    }
    override fun reset() = ring.reset()
}
