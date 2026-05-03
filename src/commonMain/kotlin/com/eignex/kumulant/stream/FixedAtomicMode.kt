@file:OptIn(ExperimentalAtomicApi::class)

package com.eignex.kumulant.stream

import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.math.pow
import kotlin.concurrent.atomics.AtomicLong as KAtomicLong
import kotlin.concurrent.atomics.AtomicLongArray as KAtomicLongArray

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
    override fun <T> newReference(initial: T): StreamRef<T> {
        rejectBoxedPrimitive(initial)
        return AtomicReference(initial)
    }

    override fun newLongArray(size: Int, init: (Int) -> Long): StreamLongArray =
        AtomicLongCellArray(KAtomicLongArray(LongArray(size, init)))

    override fun newDoubleArray(size: Int, init: (Int) -> Double): StreamDoubleArray =
        FixedAtomicDoubleArray(
            KAtomicLongArray(LongArray(size) { (init(it) * scaleLong).toLong() }),
            scaleLong,
            invScale
        )
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

    override fun compareAndSet(expectedValue: Double, newValue: Double): Boolean =
        ref.compareAndSet((expectedValue * scaleLong).toLong(), (newValue * scaleLong).toLong())
}

/** Fixed-point atomic [StreamDoubleArray] using a scaled [KAtomicLongArray]. */
class FixedAtomicDoubleArray(
    private val ref: KAtomicLongArray,
    private val scaleLong: Long,
    private val invScale: Double,
) : StreamDoubleArray {
    override val size: Int get() = ref.size

    override fun load(index: Int): Double = ref.loadAt(index) * invScale

    override fun store(index: Int, value: Double) {
        ref.storeAt(index, (value * scaleLong).toLong())
    }

    override fun add(index: Int, delta: Double) {
        ref.addAndFetchAt(index, (delta * scaleLong).toLong())
    }

    override fun addAndGet(index: Int, delta: Double): Double =
        ref.addAndFetchAt(index, (delta * scaleLong).toLong()) * invScale

    override fun getAndAdd(index: Int, delta: Double): Double =
        ref.fetchAndAddAt(index, (delta * scaleLong).toLong()) * invScale

    override fun compareAndSet(index: Int, expectedValue: Double, newValue: Double): Boolean =
        ref.compareAndSetAt(
            index,
            (expectedValue * scaleLong).toLong(),
            (newValue * scaleLong).toLong()
        )
}
