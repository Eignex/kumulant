package com.eignex.kumulant.stream

import kotlin.reflect.KProperty
import kotlin.time.TimeSource

private val monoStart = TimeSource.Monotonic.markNow()

internal fun currentTimeNanos(): Long = monoStart.elapsedNow().inWholeNanoseconds

// Timestamps use nanoseconds; rates are reported per second.
internal const val NANOS_PER_SECOND: Double = 1_000_000_000.0

// Each stat maps its Concurrency contract to cell storage and, where needed, a separate lock.
// Arrays share their scalar counterpart's per-cell guarantees without per-slot object allocation.
internal interface StreamMode {
    fun newDouble(initial: Double = 0.0): StreamDouble
    fun newLong(initial: Long = 0L): StreamLong
    fun <T> newReference(initial: T): StreamRef<T>
    fun newLongArray(size: Int, init: (Int) -> Long = { 0L }): StreamLongArray
    fun newDoubleArray(size: Int, init: (Int) -> Double = { 0.0 }): StreamDoubleArray
}

// Reference CAS compares identity, so boxed numeric values must use primitive cells instead.
internal fun rejectBoxedPrimitive(initial: Any?) {
    require(initial !is Double && initial !is Long) {
        "StreamRef does not support boxed Double/Long - use newDouble/newLong instead, " +
            "since CAS on boxed primitives compares identity, not value."
    }
}

// Operations follow the owning mode's guarantees. Striped adders provide best-effort totals
// and do not support CAS; atomic Double cells compare raw IEEE-754 bits.
internal interface StreamDouble {
    fun load(): Double
    fun store(value: Double)
    fun add(delta: Double)
    fun addAndGet(delta: Double): Double
    fun compareAndSet(expectedValue: Double, newValue: Double): Boolean
}

internal interface StreamLong {
    fun load(): Long
    fun store(value: Long)
    fun add(delta: Long)
    fun addAndGet(delta: Long): Long
    fun compareAndSet(expectedValue: Long, newValue: Long): Boolean
}

// Concurrent modes compare referents by identity; compareAndExchange returns the prior referent.
internal interface StreamRef<T> {
    fun load(): T
    fun store(value: T)
    fun compareAndExchange(expectedValue: T, newValue: T): T
    fun compareAndSet(expectedValue: T, newValue: T): Boolean
}

// Array operations have the same per-cell semantics as scalar cells; indices must be in 0 until size.
internal interface StreamDoubleArray {
    val size: Int
    fun load(index: Int): Double
    fun store(index: Int, value: Double)
    fun add(index: Int, delta: Double)
    fun compareAndSet(index: Int, expectedValue: Double, newValue: Double): Boolean
}

internal interface StreamLongArray {
    val size: Int
    fun load(index: Int): Long
    fun store(index: Int, value: Long)
    fun add(index: Int, delta: Long)
    fun addAndGet(index: Int, delta: Long): Long
    fun compareAndSet(index: Int, expectedValue: Long, newValue: Long): Boolean
}

internal operator fun StreamDouble.getValue(thisRef: Any?, property: KProperty<*>): Double = load()

internal operator fun StreamLong.getValue(thisRef: Any?, property: KProperty<*>): Long = load()
