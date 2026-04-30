package com.eignex.kumulant.stream

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.Stat
import kotlin.time.Duration

/**
 * Tumbling-slice ring buffer of per-slice [Stat] accumulators, used by the windowed
 * stat adapters across all four modalities. Holds a fixed number of [slices], each
 * owning its own accumulator built from [factory]. Slot rotation is lock-free via CAS
 * against the slot's start timestamp.
 *
 * Companion to [ArrayBins] for the windowed family — both are mode-agnostic
 * concurrent containers used to build higher-level stat operators.
 */
class SliceRing<R : Result, S : Stat<R>>(
    windowDuration: Duration,
    slices: Int,
    private val mode: StreamMode,
    private val factory: (StreamMode) -> S,
) {
    private val sliceDurationNanos: Long
    val windowDurationNanos: Long

    init {
        require(slices > 0) { "SliceRing requires at least 1 slice" }
        windowDurationNanos = windowDuration.inWholeNanoseconds
        require(windowDurationNanos > 0L) { "SliceRing requires a positive duration" }
        sliceDurationNanos = windowDurationNanos / slices
        require(sliceDurationNanos > 0L) {
            "SliceRing requires at least 1ns per slice; decrease slices or increase duration"
        }
    }

    internal class Slot<R : Result, S : Stat<R>>(val startNanos: Long, val stat: S)

    private val buckets: Array<StreamRef<Slot<R, S>>> = Array(slices) {
        mode.newReference(Slot(Long.MIN_VALUE, factory(mode)))
    }

    private fun expectedSliceStart(timestampNanos: Long): Long =
        (timestampNanos / sliceDurationNanos) * sliceDurationNanos

    private fun bucketIndex(expectedStart: Long): Int {
        val raw = ((expectedStart / sliceDurationNanos) % buckets.size).toInt()
        return if (raw < 0) raw + buckets.size else raw
    }

    /**
     * Acquire the slot for [timestampNanos], rotating the bucket if a newer slice is due.
     * Returns null if the timestamp is older than what the bucket currently holds (the
     * write would land in a slot that has already been recycled).
     */
    fun slotFor(timestampNanos: Long): S? {
        val expectedStart = expectedSliceStart(timestampNanos)
        val bucketRef = buckets[bucketIndex(expectedStart)]
        while (true) {
            val currentSlot = bucketRef.load()
            if (currentSlot.startNanos == expectedStart) return currentSlot.stat
            if (currentSlot.startNanos < expectedStart) {
                val newSlot = Slot<R, S>(expectedStart, factory(mode))
                if (bucketRef.compareAndSet(currentSlot, newSlot)) return newSlot.stat
            } else {
                return null
            }
        }
    }

    /** Merge [values] into the slot at "now", rotating the bucket first if needed. */
    fun mergeNow(values: R) {
        val expectedStart = expectedSliceStart(currentTimeNanos())
        val bucketRef = buckets[bucketIndex(expectedStart)]
        var currentSlot = bucketRef.load()
        if (currentSlot.startNanos < expectedStart) {
            val newSlot = Slot<R, S>(expectedStart, factory(mode))
            currentSlot = if (bucketRef.compareAndSet(currentSlot, newSlot)) newSlot else bucketRef.load()
        }
        currentSlot.stat.merge(values)
    }

    /** Invoke [action] on each slot stat whose start lies in `[timestampNanos - window, timestampNanos]`. */
    fun forEachActive(timestampNanos: Long, action: (S) -> Unit) {
        val cutoff = timestampNanos - windowDurationNanos
        for (bucketRef in buckets) {
            val slot = bucketRef.load()
            if (slot.startNanos in cutoff..timestampNanos) action(slot.stat)
        }
    }

    fun reset() {
        for (bucketRef in buckets) {
            bucketRef.store(Slot(Long.MIN_VALUE, factory(mode)))
        }
    }
}
