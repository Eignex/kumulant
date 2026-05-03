package com.eignex.kumulant.stream

import kotlin.reflect.KProperty
import kotlin.time.TimeSource

private val monoStart = TimeSource.Monotonic.markNow()

internal fun currentTimeNanos(): Long = monoStart.elapsedNow().inWholeNanoseconds

/**
 * Factory for the mutable scalar cells that back stat accumulators.
 *
 * The chosen mode controls a stat's thread-safety semantics:
 *
 * - [SerialMode] (default): single-threaded, no synchronisation. Cheapest path; safe only
 *   when one thread updates the stat at a time.
 * - [AtomicMode]: each cell is individually atomic via CAS. Multi-cell accumulators with
 *   coupled recurrences (e.g. Welford `(W, mean, M2)`) may drift under contention since
 *   updates aren't serialised across cells. Bounded-array sketches (Reservoir, TDigest,
 *   SpaceSaving) self-serialise internally via a private CAS spin-mutex, so concurrent
 *   `update`/`read` is safe under any mode but throughput-bound under contention.
 * - [FixedAtomicMode]: like [AtomicMode] but stores doubles as fixed-point Longs to
 *   eliminate floating-point drift on long-running additive sums.
 * - [com.eignex.kumulant.stream.AdderMode] (JVM): striped counters for write-heavy
 *   workloads where read frequency is low.
 *
 * For strict thread-safety on any stat — exact arithmetic and consistent reads under
 * arbitrary concurrency — wrap with `.locked()` (JVM, in `com.eignex.kumulant.locked`).
 * It serialises all `update`/`merge`/`read`/`reset` calls through a
 * `ReentrantReadWriteLock`. Slower than [AtomicMode] under contention, but works with
 * any stat regardless of internal concurrency support.
 */
interface StreamMode {
    /** Allocate a [StreamDouble] cell seeded to [initial]. */
    fun newDouble(initial: Double = 0.0): StreamDouble

    /** Allocate a [StreamLong] cell seeded to [initial]. */
    fun newLong(initial: Long = 0L): StreamLong

    /** Allocate a [StreamRef] cell holding [initial]. */
    fun <T> newReference(initial: T): StreamRef<T>

    /**
     * Allocate a fixed-length array of `Long` cells, initialised by [init]. Each slot
     * has the same atomicity guarantees as a single [StreamLong] under this mode but
     * with a flat backing — one allocation, no per-slot object headers.
     */
    fun newLongArray(size: Int, init: (Int) -> Long = { 0L }): StreamLongArray

    /**
     * Allocate a fixed-length array of `Double` cells, initialised by [init]. Each slot
     * has the same atomicity guarantees as a single [StreamDouble] under this mode.
     */
    fun newDoubleArray(size: Int, init: (Int) -> Double = { 0.0 }): StreamDoubleArray
}

/**
 * Guard against `StreamRef<Double>` / `StreamRef<Long>`: boxed primitives break
 * identity-based CAS (every box is a fresh instance). Callers should use
 * [StreamMode.newDouble] / [StreamMode.newLong] instead.
 */
internal fun rejectBoxedPrimitive(initial: Any?) {
    require(initial !is Double && initial !is Long) {
        "StreamRef does not support boxed Double/Long — use newDouble/newLong instead, " +
            "since CAS on boxed primitives compares identity, not value."
    }
}

/** Mutable `Double` cell with mode-appropriate read/write/add semantics. */
interface StreamDouble {
    /** Read the current value. */
    fun load(): Double

    /** Overwrite the cell with [value]. */
    fun store(value: Double)

    /** Add [delta] in place. */
    fun add(delta: Double)

    /** Add [delta] and return the new value. */
    fun addAndGet(delta: Double): Double

    /** Add [delta] and return the value before the add. */
    fun getAndAdd(delta: Double): Double

    /** Atomic compare-and-set on the IEEE-754 bit pattern; returns true iff the swap happened. */
    fun compareAndSet(expectedValue: Double, newValue: Double): Boolean
}

/** Mutable `Long` cell with mode-appropriate read/write/add semantics. */
interface StreamLong {
    /** Read the current value. */
    fun load(): Long

    /** Overwrite the cell with [value]. */
    fun store(value: Long)

    /** Add [delta] in place. */
    fun add(delta: Long)

    /** Add [delta] and return the new value. */
    fun addAndGet(delta: Long): Long

    /** Add [delta] and return the value before the add. */
    fun getAndAdd(delta: Long): Long

    /** Atomic compare-and-set; returns true iff the swap happened. */
    fun compareAndSet(expectedValue: Long, newValue: Long): Boolean
}

/**
 * Mutable reference cell with CAS semantics.
 *
 * Note: JVM boxing means `StreamRef<Double>` used under [AtomicMode] compares boxed
 * identities — avoid CAS loops on boxed primitives, prefer [StreamDouble] instead.
 */
interface StreamRef<T> {
    /** Read the current referent. */
    fun load(): T

    /** Overwrite the referent with [value]. */
    fun store(value: T)

    /** Atomic compare-and-exchange; returns the witnessed prior value. */
    fun compareAndExchange(expectedValue: T, newValue: T): T

    /** Atomic compare-and-set; returns true iff the swap happened. */
    fun compareAndSet(expectedValue: T, newValue: T): Boolean
}

/**
 * Fixed-length array of `Long` cells. Each slot supports the same load / store / add /
 * CAS operations as a scalar [StreamLong] under the owning mode. Slot indices are
 * `0..size-1`; out-of-range access throws.
 */
interface StreamLongArray {
    /** Number of cells in the array. */
    val size: Int

    /** Read the value at [index]. */
    fun load(index: Int): Long

    /** Overwrite the value at [index]. */
    fun store(index: Int, value: Long)

    /** Add [delta] in place at [index]. */
    fun add(index: Int, delta: Long)

    /** Add [delta] at [index] and return the new value. */
    fun addAndGet(index: Int, delta: Long): Long

    /** Add [delta] at [index] and return the value before the add. */
    fun getAndAdd(index: Int, delta: Long): Long

    /** Atomic compare-and-set at [index]; returns true iff the swap happened. */
    fun compareAndSet(index: Int, expectedValue: Long, newValue: Long): Boolean
}

/**
 * Fixed-length array of `Double` cells. Each slot supports the same load / store / add /
 * CAS operations as a scalar [StreamDouble] under the owning mode. Slot indices are
 * `0..size-1`; out-of-range access throws.
 */
interface StreamDoubleArray {
    /** Number of cells in the array. */
    val size: Int

    /** Read the value at [index]. */
    fun load(index: Int): Double

    /** Overwrite the value at [index]. */
    fun store(index: Int, value: Double)

    /** Add [delta] in place at [index]. */
    fun add(index: Int, delta: Double)

    /** Add [delta] at [index] and return the new value. */
    fun addAndGet(index: Int, delta: Double): Double

    /** Add [delta] at [index] and return the value before the add. */
    fun getAndAdd(index: Int, delta: Double): Double

    /** Atomic compare-and-set on the IEEE-754 bit pattern at [index]. */
    fun compareAndSet(index: Int, expectedValue: Double, newValue: Double): Boolean
}

/** Property-delegate getter for [StreamDouble] — `val x: Double by streamDouble`. */
operator fun StreamDouble.getValue(
    thisRef: Any?,
    property: KProperty<*>
): Double = load()

/** Property-delegate getter for [StreamLong] — `val x: Long by streamLong`. */
operator fun StreamLong.getValue(
    thisRef: Any?,
    property: KProperty<*>
): Long = load()
