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
 * - Apple / Linux native: backed by a `pthread_mutex_t` allocated via cinterop;
 *   the native handle is freed on GC via `kotlin.native.ref.createCleaner`.
 * - mingwX64: backed by a Win32 `CRITICAL_SECTION`, similarly cleanered.
 * - JS / Wasm: noop — these runtimes are single-threaded.
 */
expect class PlatformStreamLock() : StreamLock {
    override fun <R> withLock(block: () -> R): R
}
