package com.eignex.kumulant.stream

import kotlin.reflect.KProperty
import kotlin.time.TimeSource

private val monoStart = TimeSource.Monotonic.markNow()

internal fun currentTimeNanos(): Long = monoStart.elapsedNow().inWholeNanoseconds

/**
 * Factory for the mutable scalar cells that back stat accumulators.
 *
 * Choose based on concurrency needs: [SerialMode] for single-threaded, [AtomicMode]
 * for contended multi-writer CAS, [FixedAtomicMode] for fixed-point reduction of
 * floating-point drift, [com.eignex.kumulant.stream.AdderMode] (JVM) for
 * write-heavy counters.
 */
interface StreamMode {
    /** Allocate a [StreamDouble] cell seeded to [initial]. */
    fun newDouble(initial: Double = 0.0): StreamDouble

    /** Allocate a [StreamLong] cell seeded to [initial]. */
    fun newLong(initial: Long = 0L): StreamLong

    /** Allocate a [StreamRef] cell holding [initial]. */
    fun <T> newReference(initial: T): StreamRef<T>
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
