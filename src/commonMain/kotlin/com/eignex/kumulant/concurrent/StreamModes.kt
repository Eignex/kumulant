@file:OptIn(ExperimentalAtomicApi::class)

package com.eignex.kumulant.concurrent

import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.jvm.JvmInline
import kotlin.math.pow
import kotlin.reflect.KProperty
import kotlin.concurrent.atomics.AtomicLong as KAtomicLong
import kotlin.concurrent.atomics.AtomicReference as KAtomicReference

/**
 * Factory for the mutable scalar cells that back stat accumulators.
 *
 * Choose based on concurrency needs: [SerialMode] for single-threaded, [AtomicMode]
 * for contended multi-writer CAS, [FixedAtomicMode] for fixed-point reduction of
 * floating-point drift, [com.eignex.kumulant.concurrent.AdderMode] (JVM) for
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

/**
 * Fixed-point atomic mode: doubles are encoded as `Long` scaled by `10^[precision]`.
 *
 * Exact addition (no FP drift) and a tight atomic footprint, but the dynamic range is
 * clipped to what fits in 63 bits after scaling — pick [precision] deliberately.
 */
class FixedAtomicMode(precision: Int) : StreamMode {
    private val scale: Double = 10.0.pow(precision)
    private val scaleLong: Long = scale.toLong()
    private val invScale: Double = 1.0 / scale

    override fun newDouble(initial: Double) = FixedAtomicDouble(
        (initial * scaleLong).toLong(),
        scaleLong,
        invScale
    )

    override fun newLong(initial: Long) = AtomicLong(initial)
    override fun <T> newReference(initial: T): StreamRef<T> =
        AtomicReference(initial)
}

/** Atomic mode backed by platform atomics; CAS-based, safe for concurrent updates. */
object AtomicMode : StreamMode {
    override fun newDouble(initial: Double) = AtomicDouble(initial)
    override fun newLong(initial: Long) = AtomicLong(initial)
    override fun <T> newReference(initial: T) = AtomicReference<T>(initial)
}

/** Non-atomic, single-threaded mode; cheapest path when no concurrency is required. */
object SerialMode : StreamMode {
    override fun newDouble(initial: Double) = SerialDouble(initial)
    override fun newLong(initial: Long) = SerialLong(initial)
    override fun <T> newReference(initial: T) = SerialRef(initial)
}

/** Plain-`var` [StreamLong] implementation used by [SerialMode]. */
class SerialLong(var ref: Long) : StreamLong {
    override fun load(): Long = ref

    override fun store(value: Long) {
        ref = value
    }

    override fun add(delta: Long) {
        ref += delta
    }

    override fun addAndGet(delta: Long): Long {
        ref += delta
        return ref
    }

    override fun getAndAdd(delta: Long): Long {
        val ret = ref
        ref += delta
        return ret
    }
}

/** Plain-`var` [StreamDouble] implementation used by [SerialMode]. */
class SerialDouble(var ref: Double) : StreamDouble {
    override fun load(): Double = ref

    override fun store(value: Double) {
        ref = value
    }

    override fun add(delta: Double) {
        ref += delta
    }

    override fun addAndGet(delta: Double): Double {
        ref += delta
        return ref
    }

    override fun getAndAdd(delta: Double): Double {
        val ret = ref
        ref += delta
        return ret
    }
}

/** Plain-`var` [StreamRef] implementation used by [SerialMode]. */
class SerialRef<T>(var ref: T) : StreamRef<T> {
    override fun load(): T = ref

    override fun store(value: T) {
        ref = value
    }

    override fun compareAndExchange(expectedValue: T, newValue: T): T {
        if (ref === expectedValue) {
            ref = newValue
            return expectedValue
        } else {
            return ref
        }
    }

    override fun compareAndSet(expectedValue: T, newValue: T): Boolean {
        if (ref === expectedValue) {
            ref = newValue
            return true
        } else {
            return false
        }
    }
}

/** Fixed-point atomic [StreamDouble] using a scaled `Long` under the hood. */
class FixedAtomicDouble(
    initialLong: Long,
    private val scaleLong: Long,
    private val invScale: Double
) : StreamDouble {

    private val ref = KAtomicLong(initialLong)

    override fun load(): Double = ref.load() * invScale

    override fun store(value: Double) =
        ref.store((value * scaleLong).toLong())

    override fun add(delta: Double) {
        ref.addAndFetch((delta * scaleLong).toLong())
    }

    override fun addAndGet(delta: Double): Double {
        return ref.addAndFetch((delta * scaleLong).toLong()) * invScale
    }

    override fun getAndAdd(delta: Double): Double {
        return ref.fetchAndAdd((delta * scaleLong).toLong()) * invScale
    }
}

/** CAS-based atomic [StreamDouble] using `Double.toRawBits` encoding over an atomic `Long`. */
@JvmInline
value class AtomicDouble(val ref: KAtomicLong) : StreamDouble {

    constructor(initial: Double = 0.0) : this(KAtomicLong(initial.toRawBits()))

    override fun load() = Double.fromBits(ref.load())

    override fun store(value: Double) {
        ref.store(value.toRawBits())
    }

    override fun add(delta: Double) {
        if (delta == 0.0) return
        var currentBits = ref.load()
        while (true) {
            val currentVal = Double.fromBits(currentBits)
            val nextBits = (currentVal + delta).toRawBits()
            if (currentBits == nextBits) return

            val witness = ref.compareAndExchange(currentBits, nextBits)
            if (witness == currentBits) break
            currentBits = witness
        }
    }

    override fun addAndGet(delta: Double): Double {
        var currentBits = ref.load()
        while (true) {
            val currentVal = Double.fromBits(currentBits)
            val nextVal = currentVal + delta
            val nextBits = nextVal.toRawBits()
            if (currentBits == nextBits) return nextVal

            val witness = ref.compareAndExchange(currentBits, nextBits)
            if (witness == currentBits) return nextVal
            currentBits = witness
        }
    }

    override fun getAndAdd(delta: Double): Double {
        var currentBits = ref.load()
        while (true) {
            val currentVal = Double.fromBits(currentBits)
            val nextBits = (currentVal + delta).toRawBits()
            if (currentBits == nextBits) return currentVal

            val witness = ref.compareAndExchange(currentBits, nextBits)
            if (witness == currentBits) return currentVal
            currentBits = witness
        }
    }
}

/** Platform-atomic [StreamLong]. */
@JvmInline
value class AtomicLong(val ref: KAtomicLong) : StreamLong {
    constructor(initial: Long = 0L) : this(KAtomicLong(initial))

    override fun load(): Long = ref.load()

    override fun store(value: Long) {
        ref.store(value)
    }

    override fun add(delta: Long) {
        ref.addAndFetch(delta)
    }

    override fun addAndGet(delta: Long): Long {
        return ref.addAndFetch(delta)
    }

    override fun getAndAdd(delta: Long): Long {
        return ref.fetchAndAdd(delta)
    }
}

/** Platform-atomic [StreamRef]. */
@JvmInline
value class AtomicReference<T>(val ref: KAtomicReference<T>) : StreamRef<T> {
    constructor(value: T) : this(KAtomicReference(value))

    override fun load(): T = ref.load()

    override fun store(value: T) {
        ref.store(value)
    }

    override fun compareAndExchange(expectedValue: T, newValue: T): T {
        return ref.compareAndExchange(expectedValue, newValue)
    }

    override fun compareAndSet(expectedValue: T, newValue: T): Boolean {
        return ref.compareAndSet(expectedValue, newValue)
    }
}
