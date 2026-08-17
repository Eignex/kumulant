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
 * Companion to [ArrayBins] for the windowed family - both are mode-agnostic
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
                // Bucket already advanced past us - the slot we'd want has been recycled.
                return
            }
            val newSlot = Slot<R, S>(expectedStart, factory(concurrency))
            if (bucketRef.compareAndSet(currentSlot, newSlot)) {
                newSlot.stat.merge(values)
                return
            }
            // Lost CAS - retry; another thread may have installed the same or a newer slot.
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

/**
 * Slices a sliding window is divided into when the caller does not say.
 *
 * Declared twelve times: four live `Windowed*Stat` classes, four `windowed` DSL functions, and four
 * `Windowed*` wire specs. The live and wire copies must agree or a materialised spec windows differently
 * than the equivalent direct call, which is the same class of divergence that made a spec-built EXP3
 * behave unlike a directly-built one.
 *
 * Ten is a resolution choice, not a tuning constant: it fixes the granularity at which the window can
 * expire, so a one-minute window drops observations in six-second steps. More slices means finer expiry
 * and proportionally more per-slice state.
 */
internal const val DEFAULT_WINDOW_SLICES: Int = 10

/**
 * Bounds a min-max scaler projects onto when the caller does not say.
 *
 * Declared thirteen times across the AST node, the four specs, the four DSL functions and the four live
 * scalers. The unit interval is the only default that makes the scaler composable with the probability
 * consumers - calibration, log loss, AUC - which all require `[0, 1]`.
 */
internal const val DEFAULT_TARGET_LOW: Double = 0.0

/** Upper end of [DEFAULT_TARGET_LOW]'s interval. */
internal const val DEFAULT_TARGET_HIGH: Double = 1.0
