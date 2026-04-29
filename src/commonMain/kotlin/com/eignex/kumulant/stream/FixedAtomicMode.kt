@file:OptIn(ExperimentalAtomicApi::class)

package com.eignex.kumulant.stream

import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.math.pow
import kotlin.concurrent.atomics.AtomicLong as KAtomicLong

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
