package com.eignex.kumulant.operation

import com.eignex.kumulant.concurrent.SerialMode
import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.StreamRef
import com.eignex.kumulant.concurrent.currentTimeNanos
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import kotlin.time.Duration

fun <R : Result> SeriesStat<R>.windowed(
    duration: Duration,
    slices: Int = 10,
    mode: StreamMode = defaultStreamMode
): SeriesStat<R> {
    return WindowedSeriesStat(duration, slices, this, mode)
}

class WindowedSeriesStat<R : Result>(
    private val windowDuration: Duration,
    private val slices: Int,
    template: SeriesStat<R>,
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<R> {

    private val template = template.create(mode = this.mode)

    init {
        require(slices > 0) { "WindowedStat requires at least 1 slice" }
    }

    private val sliceDurationNanos = windowDuration.inWholeNanoseconds / slices

    // Holds a fixed time boundary and the specific stat for that slice
    private class Slot<R : Result>(
        val startNanos: Long,
        val stat: SeriesStat<R>
    )

    // A ring buffer of atomic references, initialized with empty slots
    private val buckets = Array<StreamRef<Slot<R>>>(slices) {
        mode.newReference(Slot(Long.MIN_VALUE, template.create(mode = this.mode)))
    }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        // 1. Map the timestamp to a fixed mathematical grid
        val expectedStart = (timestampNanos / sliceDurationNanos) * sliceDurationNanos
        val bucketIndex = ((expectedStart / sliceDurationNanos) % slices).toInt()

        // Handle negative modulo if timestamps pre-date the Unix Epoch
        val safeIndex = if (bucketIndex < 0) bucketIndex + slices else bucketIndex

        val bucketRef = buckets[safeIndex]

        while (true) {
            val currentSlot = bucketRef.load()

            // 2. If the slot matches the current time grid, update it and exit
            if (currentSlot.startNanos == expectedStart) {
                currentSlot.stat.update(value, timestampNanos, weight)
                return
            }

            // 3. If the slot is too old, allocate a fresh one and try to swap it in (Lock-free rotation)
            if (currentSlot.startNanos < expectedStart) {
                val newSlot = Slot(expectedStart, template.create(mode = mode))
                if (bucketRef.compareAndSet(currentSlot, newSlot)) {
                    newSlot.stat.update(value, timestampNanos, weight)
                    return
                }
                // If CAS failed, another thread rotated it. Loop again.
            } else {
                // The event is older than the active window for this bucket index.
                // We drop it (or you could choose to ignore out-of-order late events).
                return
            }
        }
    }

    override fun create(mode: StreamMode?): SeriesStat<R> =
        WindowedSeriesStat(windowDuration, slices, template, mode ?: this.mode)

    override fun read(timestampNanos: Long): R {
        // Create an empty accumulator using the factory
        val accumulator = template.create(mode = SerialMode)
        val cutoffNanos = timestampNanos - windowDuration.inWholeNanoseconds

        // Iterate through all buckets and merge the valid ones
        for (bucketRef in buckets) {
            val slot = bucketRef.load()
            // Only merge if the bucket's time slice hasn't expired
            if (slot.startNanos >= cutoffNanos) {
                accumulator.merge(slot.stat.read(timestampNanos))
            }
        }

        return accumulator.read(timestampNanos)
    }

    override fun merge(values: R) {
        // To merge incoming states safely, we drop the incoming state into the current "now" bucket.
        // This is an approximation for distributed merging, but standard for stream windowing.
        val now = currentTimeNanos()
        val expectedStart = (now / sliceDurationNanos) * sliceDurationNanos
        val bucketIndex = ((expectedStart / sliceDurationNanos) % slices).toInt()
        val safeIndex = if (bucketIndex < 0) bucketIndex + slices else bucketIndex

        val bucketRef = buckets[safeIndex]

        // Ensure the bucket is rotated to "now" before merging
        var currentSlot = bucketRef.load()
        if (currentSlot.startNanos < expectedStart) {
            val newSlot = Slot(expectedStart, template.create(mode = mode))
            if (bucketRef.compareAndSet(currentSlot, newSlot)) {
                currentSlot = newSlot
            } else {
                currentSlot = bucketRef.load()
            }
        }

        currentSlot.stat.merge(values)
    }

    override fun reset() {
        for (bucketRef in buckets) {
            bucketRef.store(Slot(Long.MIN_VALUE, template.create(mode = mode)))
        }
    }
}
