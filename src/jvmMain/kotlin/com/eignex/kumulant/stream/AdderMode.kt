package com.eignex.kumulant.stream

import java.util.concurrent.atomic.DoubleAdder as JDoubleAdder
import java.util.concurrent.atomic.LongAdder as JLongAdder

/**
 * JVM [StreamMode] backed by `java.util.concurrent.atomic.DoubleAdder` / `LongAdder`.
 *
 * Optimized for many concurrent writers (striped cells). `load()` sweeps all cells and
 * is slower than CAS atomics, so prefer this for write-heavy counters read infrequently.
 * `load` is non-linearizable: use [AtomicMode] if reads must observe a single update
 * atomically with subsequent writes.
 */
object AdderMode : StreamMode {
    override fun newDouble(initial: Double) = DoubleAdder(initial)
    override fun newLong(initial: Long) = LongAdder(initial)
    override fun <T> newReference(initial: T): AtomicReference<T> {
        rejectBoxedPrimitive(initial)
        return AtomicReference(initial)
    }

    /**
     * Falls back to the [AtomicMode] flat-array backing. A per-slot striped adder for
     * O(N) sketches with thousands of bins would balloon memory; consistent with the
     * scalar `compareAndSet` throwing on `DoubleAdder`/`LongAdder`, the array path
     * just delegates to single-cell atomics.
     */
    override fun newLongArray(size: Int, init: (Int) -> Long): StreamLongArray =
        AtomicMode.newLongArray(size, init)

    override fun newDoubleArray(size: Int, init: (Int) -> Double): StreamDoubleArray =
        AtomicMode.newDoubleArray(size, init)
}

/** [StreamDouble] backed by a striped `java.util.concurrent.atomic.DoubleAdder`. */
@JvmInline
value class DoubleAdder(val ref: JDoubleAdder) : StreamDouble {

    constructor(initial: Double = 0.0) : this(
        JDoubleAdder().also {
            it.add(initial)
        }
    )

    override fun load(): Double {
        return ref.sum()
    }

    override fun store(value: Double) {
        ref.reset()
        ref.add(value)
    }

    override fun add(delta: Double) {
        ref.add(delta)
    }

    override fun addAndGet(delta: Double): Double {
        ref.add(delta)
        return ref.sum()
    }

    override fun getAndAdd(delta: Double): Double {
        val ret = ref.sum()
        ref.add(delta)
        return ret
    }

    override fun compareAndSet(expectedValue: Double, newValue: Double): Boolean {
        throw UnsupportedOperationException(
            "DoubleAdder does not support compareAndSet; use AtomicMode for CAS-based stats"
        )
    }
}

/** [StreamLong] backed by a striped `java.util.concurrent.atomic.LongAdder`. */
@JvmInline
value class LongAdder(val ref: JLongAdder) : StreamLong {
    constructor(initial: Long = 0L) : this(JLongAdder().also { it.add(initial) })

    override fun load(): Long {
        return ref.sum()
    }

    override fun store(value: Long) {
        ref.reset()
        ref.add(value)
    }

    override fun add(delta: Long) {
        ref.add(delta)
    }

    override fun addAndGet(delta: Long): Long {
        ref.add(delta)
        return ref.sum()
    }

    override fun getAndAdd(delta: Long): Long {
        val ret = ref.sum()
        ref.add(delta)
        return ret
    }

    override fun compareAndSet(expectedValue: Long, newValue: Long): Boolean {
        throw UnsupportedOperationException(
            "LongAdder does not support compareAndSet; use AtomicMode for CAS-based stats"
        )
    }
}
