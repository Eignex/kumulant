package com.eignex.kumulant.stream

import com.eignex.kumulant.core.Concurrency

/**
 * Mutual-exclusion lock used by stats whose update logic spans multiple cells or
 * multiple steps and therefore can't be made elementwise atomic. Obtain one for a
 * given [Concurrency] level via [Concurrency.lock]: a no-op under [Concurrency.None]
 * (single-threaded, zero overhead), a platform mutex otherwise.
 */
interface Mutex {
    /** Run [block] holding the lock and return its result. */
    fun <R> withLock(block: () -> R): R
}

/**
 * The lock for a [Concurrency] level: [NoopMutex] under [Concurrency.None] — a
 * single-threaded owner pays no synchronization — and a [PlatformMutex] under any
 * concurrent level. The same contract the per-stat `serializedLock` uses, exposed for
 * callers that need a coherent lock for their own coupled state.
 */
fun Concurrency.lock(): Mutex = when (this) {
    Concurrency.None -> NoopMutex
    else -> PlatformMutex()
}

/** No-op lock used under [com.eignex.kumulant.core.Concurrency.None] and on
 *  drift-tolerant paths. Avoids any synchronization cost. */
internal object NoopMutex : Mutex {
    override fun <R> withLock(block: () -> R): R = block()
}

/**
 * Platform-provided mutual-exclusion lock.
 *
 * - JVM: backed by `java.util.concurrent.locks.ReentrantLock`.
 * - Apple / Linux native: backed by a `pthread_mutex_t` allocated via cinterop;
 *   the native handle is freed on GC via `kotlin.native.ref.createCleaner`.
 * - mingwX64: backed by a Win32 `CRITICAL_SECTION`, similarly cleanered.
 * - JS / Wasm: noop - these runtimes are single-threaded.
 */
internal expect class PlatformMutex() : Mutex {
    override fun <R> withLock(block: () -> R): R
}
