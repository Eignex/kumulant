package com.eignex.kumulant.stream

/**
 * Mutual-exclusion lock used by stats whose update logic spans multiple cells or
 * multiple steps and therefore can't be made elementwise atomic. Allocate once per
 * stat instance via [PlatformStreamLock] (or [NoopStreamLock] when no lock is
 * needed).
 */
interface StreamLock {
    fun <R> withLock(block: () -> R): R
}

/** No-op lock used under [com.eignex.kumulant.core.Concurrency.None] and on
 *  drift-tolerant paths. Avoids any synchronization cost. */
object NoopStreamLock : StreamLock {
    override fun <R> withLock(block: () -> R): R = block()
}

/**
 * Platform-provided mutual-exclusion lock.
 *
 * - JVM: backed by `java.util.concurrent.locks.ReentrantLock`.
 * - Native / JS / Wasm: backed by a CAS spin-mutex on a [kotlin.concurrent.atomics.AtomicLong].
 *   Single-threaded JS/Wasm targets pay only one uncontested CAS per acquire.
 *
 * Replace the non-JVM actual with a yielding lock once one becomes available
 * (atomicfu's `SynchronizedObject`, or stdlib equivalent).
 */
expect class PlatformStreamLock() : StreamLock {
    override fun <R> withLock(block: () -> R): R
}
