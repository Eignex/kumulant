package com.eignex.kumulant.stream

import com.eignex.kumulant.core.Concurrency
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
internal class SliceRing<R : Result, S : Stat<R>>(
    windowDuration: Duration,
    slices: Int,
    private val concurrency: Concurrency,
    private val factory: (Concurrency) -> S,
) {
    private val sliceDurationNanos: Long
    val windowDurationNanos: Long

    private val ringMode: StreamMode = concurrency.additiveMode()

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
        ringMode.newReference(Slot(Long.MIN_VALUE, factory(concurrency)))
    }

    private fun expectedSliceStart(timestampNanos: Long): Long =
        timestampNanos.floorDiv(sliceDurationNanos) * sliceDurationNanos

    private fun bucketIndex(expectedStart: Long): Int {
        val raw = (expectedStart.floorDiv(sliceDurationNanos) % buckets.size).toInt()
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
                val newSlot = Slot<R, S>(expectedStart, factory(concurrency))
                if (bucketRef.compareAndSet(currentSlot, newSlot)) return newSlot.stat
            } else {
                return null
            }
        }
    }

    /** Merge [values] into the slot at "now", rotating the bucket first if needed. */
    fun mergeNow(values: R) {
        mergeAt(currentTimeNanos(), values)
    }

    /**
     * Merge [values] into the slot that owns [timestampNanos], rotating the bucket
     * first if needed. Exposed for deterministic time-driven tests; `mergeNow` is
     * the production entry point.
     */
    fun mergeAt(timestampNanos: Long, values: R) {
        val expectedStart = expectedSliceStart(timestampNanos)
        val bucketRef = buckets[bucketIndex(expectedStart)]
        while (true) {
            val currentSlot = bucketRef.load()
            if (currentSlot.startNanos == expectedStart) {
                currentSlot.stat.merge(values)
                return
            }
            if (currentSlot.startNanos > expectedStart) {
                // Bucket already advanced past us — the slot we'd want has been recycled.
                return
            }
            val newSlot = Slot<R, S>(expectedStart, factory(concurrency))
            if (bucketRef.compareAndSet(currentSlot, newSlot)) {
                newSlot.stat.merge(values)
                return
            }
            // Lost CAS — retry; another thread may have installed the same or a newer slot.
        }
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
            bucketRef.store(Slot(Long.MIN_VALUE, factory(concurrency)))
        }
    }
}
