package com.eignex.kumulant.stream

import java.util.concurrent.atomic.DoubleAdder as JDoubleAdder
import java.util.concurrent.atomic.LongAdder as JLongAdder

// Striped adders favor frequent writes and infrequent reads. Totals are best-effort; AtomicMode provides linearizable
// cells.
internal object AdderMode : StreamMode {
    override fun newDouble(initial: Double) = DoubleAdder(initial)
    override fun newLong(initial: Long) = LongAdder(initial)
    override fun <T> newReference(initial: T): AtomicReference<T> {
        rejectBoxedPrimitive(initial)
        return AtomicReference(initial)
    }

    // A striped adder per array slot would multiply the memory cost of large sketches.
    override fun newLongArray(size: Int, init: (Int) -> Long): StreamLongArray = AtomicMode.newLongArray(size, init)

    override fun newDoubleArray(size: Int, init: (Int) -> Double): StreamDoubleArray =
        AtomicMode.newDoubleArray(size, init)
}

// DoubleAdder sums from +0.0, so reported zeros are positive even when seeded with -0.0.
@JvmInline
internal value class DoubleAdder(val ref: JDoubleAdder) : StreamDouble {

    constructor(initial: Double = 0.0) : this(
        JDoubleAdder().also {
            it.add(initial)
        },
    )

    override fun load(): Double = ref.sum()

    // Resetting stripes can discard racing adds; a relative add preserves their contributions.
    // Concurrent stores and the intermediate sum remain best-effort. A non-finite current sum or
    // target has no finite delta to add, so that case resets the stripes and accepts the race.
    override fun store(value: Double) {
        val delta = value - ref.sum()
        if (delta.isFinite()) {
            ref.add(delta)
        } else {
            ref.reset()
            ref.add(value)
        }
    }

    override fun add(delta: Double) {
        if (delta == 0.0) return
        ref.add(delta)
    }

    // A racing add may be included in this total, so this cannot elect a first writer.
    // Election sites must use AtomicMode through firstWriterMode or monotonicMode.
    override fun addAndGet(delta: Double): Double {
        ref.add(delta)
        return ref.sum()
    }

    override fun compareAndSet(expectedValue: Double, newValue: Double): Boolean = throw UnsupportedOperationException(
        "DoubleAdder does not support compareAndSet; use AtomicMode for CAS-based stats",
    )
}

// `StreamLong` backed by a striped `java.util.concurrent.atomic.LongAdder`.
@JvmInline
internal value class LongAdder(val ref: JLongAdder) : StreamLong {
    constructor(initial: Long = 0L) : this(JLongAdder().also { it.add(initial) })

    override fun load(): Long = ref.sum()

    // Resetting stripes can discard racing adds; a relative add preserves their contributions.
    // Concurrent stores and the intermediate sum remain best-effort.
    override fun store(value: Long) {
        ref.add(value - ref.sum())
    }

    override fun add(delta: Long) {
        if (delta == 0L) return
        ref.add(delta)
    }

    // A racing add may be included in this total, so this cannot elect a first writer.
    // Election sites must use AtomicMode through firstWriterMode or monotonicMode.
    override fun addAndGet(delta: Long): Long {
        ref.add(delta)
        return ref.sum()
    }

    override fun compareAndSet(expectedValue: Long, newValue: Long): Boolean = throw UnsupportedOperationException(
        "LongAdder does not support compareAndSet; use AtomicMode for CAS-based stats",
    )
}
