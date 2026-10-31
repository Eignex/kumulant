package com.eignex.kumulant.stream

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.Stat
import kotlin.time.Duration

// Window reads merge per-slice accumulators, bounding storage and expiration granularity.
// Concurrent rotation publishes a fresh slot via reference CAS; each stat keeps its own mode.
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

    // A timestamp targeting a recycled slice has no writable slot.
    fun slotFor(timestampNanos: Long): S? {
        val expectedStart = expectedSliceStart(timestampNanos)
        val bucketRef = buckets[bucketIndex(expectedStart)]
        while (true) {
            val currentSlot = bucketRef.load()
            if (currentSlot.startNanos == expectedStart) return currentSlot.stat
            if (currentSlot.startNanos < expectedStart) {
                val newSlot = Slot<R, S>(expectedStart, factory(concurrency))
                if (bucketRef.compareAndSet(currentSlot, newSlot)) {
                    return newSlot.stat
                }
            } else {
                return null
            }
        }
    }

    fun mergeAt(timestampNanos: Long, values: R) {
        val expectedStart = expectedSliceStart(timestampNanos)
        val bucketRef = buckets[bucketIndex(expectedStart)]
        while (true) {
            val currentSlot = bucketRef.load()
            if (currentSlot.startNanos == expectedStart) {
                currentSlot.stat.merge(values, timestampNanos)
                return
            }
            if (currentSlot.startNanos > expectedStart) {
                return
            }
            val newSlot = Slot<R, S>(expectedStart, factory(concurrency))
            if (bucketRef.compareAndSet(currentSlot, newSlot)) {
                newSlot.stat.merge(values, timestampNanos)
                return
            }
        }
    }

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

// Shared by live adapters and wire specs: finer expiry requires proportionally more per-slice state.
internal const val DEFAULT_WINDOW_SLICES: Int = 10

// Unit-interval scaling composes with consumers that expect probabilities.
internal const val DEFAULT_TARGET_LOW: Double = 0.0

internal const val DEFAULT_TARGET_HIGH: Double = 1.0
