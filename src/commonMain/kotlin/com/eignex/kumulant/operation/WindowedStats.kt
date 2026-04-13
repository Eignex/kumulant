package com.eignex.kumulant.operation

import com.eignex.kumulant.concurrent.SerialMode
import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.StreamRef
import com.eignex.kumulant.concurrent.currentTimeNanos
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.core.PairedStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.VectorStat
import kotlin.time.Duration

fun <R : Result> SeriesStat<R>.windowed(
    duration: Duration,
    slices: Int = 10,
    mode: StreamMode = defaultStreamMode
): SeriesStat<R> {
    return WindowedSeriesStat(duration, slices, this, mode)
}

fun <R : Result> PairedStat<R>.windowed(
    duration: Duration,
    slices: Int = 10,
    mode: StreamMode = defaultStreamMode
): PairedStat<R> {
    return WindowedPairedStat(duration, slices, this, mode)
}

fun <R : Result> VectorStat<R>.windowed(
    duration: Duration,
    slices: Int = 10,
    mode: StreamMode = defaultStreamMode
): VectorStat<R> {
    return WindowedVectorStat(duration, slices, this, mode)
}

private data class WindowConfig(
    val windowDurationNanos: Long,
    val sliceDurationNanos: Long,
)

private fun windowConfig(windowDuration: Duration, slices: Int): WindowConfig {
    require(slices > 0) { "WindowedStat requires at least 1 slice" }

    val windowDurationNanos = windowDuration.inWholeNanoseconds
    require(windowDurationNanos > 0L) { "WindowedStat requires a positive duration" }

    val sliceDurationNanos = windowDurationNanos / slices
    require(sliceDurationNanos > 0L) {
        "WindowedStat requires at least 1ns per slice; decrease slices or increase duration"
    }

    return WindowConfig(windowDurationNanos, sliceDurationNanos)
}

private fun expectedSliceStart(timestampNanos: Long, sliceDurationNanos: Long): Long {
    return (timestampNanos / sliceDurationNanos) * sliceDurationNanos
}

private fun safeBucketIndex(expectedStart: Long, sliceDurationNanos: Long, slices: Int): Int {
    val bucketIndex = ((expectedStart / sliceDurationNanos) % slices).toInt()
    return if (bucketIndex < 0) bucketIndex + slices else bucketIndex
}

class WindowedSeriesStat<R : Result>(
    private val windowDuration: Duration,
    private val slices: Int,
    template: SeriesStat<R>,
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<R> {

    private val config = windowConfig(windowDuration, slices)
    private val template = template.create(mode = this.mode)
    private val windowDurationNanos = config.windowDurationNanos
    private val sliceDurationNanos = config.sliceDurationNanos

    private class Slot<R : Result>(
        val startNanos: Long,
        val stat: SeriesStat<R>
    )

    private val buckets = Array<StreamRef<Slot<R>>>(slices) {
        mode.newReference(Slot(Long.MIN_VALUE, template.create(mode = this.mode)))
    }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        val expectedStart = expectedSliceStart(timestampNanos, sliceDurationNanos)
        val safeIndex = safeBucketIndex(expectedStart, sliceDurationNanos, slices)

        val bucketRef = buckets[safeIndex]

        while (true) {
            val currentSlot = bucketRef.load()

            if (currentSlot.startNanos == expectedStart) {
                currentSlot.stat.update(value, timestampNanos, weight)
                return
            }

            if (currentSlot.startNanos < expectedStart) {
                val newSlot = Slot(expectedStart, template.create(mode = mode))
                if (bucketRef.compareAndSet(currentSlot, newSlot)) {
                    newSlot.stat.update(value, timestampNanos, weight)
                    return
                }
            } else {
                return
            }
        }
    }

    override fun create(mode: StreamMode?): SeriesStat<R> =
        WindowedSeriesStat(windowDuration, slices, template, mode ?: this.mode)

    override fun read(timestampNanos: Long): R {
        val accumulator = template.create(mode = SerialMode)
        val cutoffNanos = timestampNanos - windowDurationNanos

        for (bucketRef in buckets) {
            val slot = bucketRef.load()
            if (slot.startNanos >= cutoffNanos && slot.startNanos <= timestampNanos) {
                accumulator.merge(slot.stat.read(timestampNanos))
            }
        }

        return accumulator.read(timestampNanos)
    }

    override fun merge(values: R) {
        val now = currentTimeNanos()
        val expectedStart = expectedSliceStart(now, sliceDurationNanos)
        val safeIndex = safeBucketIndex(expectedStart, sliceDurationNanos, slices)

        val bucketRef = buckets[safeIndex]

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

class WindowedPairedStat<R : Result>(
    private val windowDuration: Duration,
    private val slices: Int,
    template: PairedStat<R>,
    val mode: StreamMode = defaultStreamMode,
) : PairedStat<R> {

    private val config = windowConfig(windowDuration, slices)
    private val template = template.create(mode = this.mode)
    private val windowDurationNanos = config.windowDurationNanos
    private val sliceDurationNanos = config.sliceDurationNanos

    private class Slot<R : Result>(
        val startNanos: Long,
        val stat: PairedStat<R>
    )

    private val buckets = Array<StreamRef<Slot<R>>>(slices) {
        mode.newReference(Slot(Long.MIN_VALUE, template.create(mode = this.mode)))
    }

    override fun update(x: Double, y: Double, timestampNanos: Long, weight: Double) {
        val expectedStart = expectedSliceStart(timestampNanos, sliceDurationNanos)
        val safeIndex = safeBucketIndex(expectedStart, sliceDurationNanos, slices)
        val bucketRef = buckets[safeIndex]

        while (true) {
            val currentSlot = bucketRef.load()
            if (currentSlot.startNanos == expectedStart) {
                currentSlot.stat.update(x, y, timestampNanos, weight)
                return
            }

            if (currentSlot.startNanos < expectedStart) {
                val newSlot = Slot(expectedStart, template.create(mode = mode))
                if (bucketRef.compareAndSet(currentSlot, newSlot)) {
                    newSlot.stat.update(x, y, timestampNanos, weight)
                    return
                }
            } else {
                return
            }
        }
    }

    override fun create(mode: StreamMode?): PairedStat<R> =
        WindowedPairedStat(windowDuration, slices, template, mode ?: this.mode)

    override fun read(timestampNanos: Long): R {
        val accumulator = template.create(mode = SerialMode)
        val cutoffNanos = timestampNanos - windowDurationNanos

        for (bucketRef in buckets) {
            val slot = bucketRef.load()
            if (slot.startNanos >= cutoffNanos && slot.startNanos <= timestampNanos) {
                accumulator.merge(slot.stat.read(timestampNanos))
            }
        }

        return accumulator.read(timestampNanos)
    }

    override fun merge(values: R) {
        val now = currentTimeNanos()
        val expectedStart = expectedSliceStart(now, sliceDurationNanos)
        val safeIndex = safeBucketIndex(expectedStart, sliceDurationNanos, slices)
        val bucketRef = buckets[safeIndex]

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

class WindowedVectorStat<R : Result>(
    private val windowDuration: Duration,
    private val slices: Int,
    template: VectorStat<R>,
    val mode: StreamMode = defaultStreamMode,
) : VectorStat<R> {

    private val config = windowConfig(windowDuration, slices)
    private val template = template.create(mode = this.mode)
    private val windowDurationNanos = config.windowDurationNanos
    private val sliceDurationNanos = config.sliceDurationNanos

    private class Slot<R : Result>(
        val startNanos: Long,
        val stat: VectorStat<R>
    )

    private val buckets = Array<StreamRef<Slot<R>>>(slices) {
        mode.newReference(Slot(Long.MIN_VALUE, template.create(mode = this.mode)))
    }

    override fun update(vector: DoubleArray, timestampNanos: Long, weight: Double) {
        val expectedStart = expectedSliceStart(timestampNanos, sliceDurationNanos)
        val safeIndex = safeBucketIndex(expectedStart, sliceDurationNanos, slices)
        val bucketRef = buckets[safeIndex]

        while (true) {
            val currentSlot = bucketRef.load()
            if (currentSlot.startNanos == expectedStart) {
                currentSlot.stat.update(vector, timestampNanos, weight)
                return
            }

            if (currentSlot.startNanos < expectedStart) {
                val newSlot = Slot(expectedStart, template.create(mode = mode))
                if (bucketRef.compareAndSet(currentSlot, newSlot)) {
                    newSlot.stat.update(vector, timestampNanos, weight)
                    return
                }
            } else {
                return
            }
        }
    }

    override fun create(mode: StreamMode?): VectorStat<R> =
        WindowedVectorStat(windowDuration, slices, template, mode ?: this.mode)

    override fun read(timestampNanos: Long): R {
        val accumulator = template.create(mode = SerialMode)
        val cutoffNanos = timestampNanos - windowDurationNanos

        for (bucketRef in buckets) {
            val slot = bucketRef.load()
            if (slot.startNanos >= cutoffNanos && slot.startNanos <= timestampNanos) {
                accumulator.merge(slot.stat.read(timestampNanos))
            }
        }

        return accumulator.read(timestampNanos)
    }

    override fun merge(values: R) {
        val now = currentTimeNanos()
        val expectedStart = expectedSliceStart(now, sliceDurationNanos)
        val safeIndex = safeBucketIndex(expectedStart, sliceDurationNanos, slices)
        val bucketRef = buckets[safeIndex]

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
