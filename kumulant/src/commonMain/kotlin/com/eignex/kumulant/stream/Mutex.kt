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

    /**
     * Acquire the lock. Pair with [exit] in a `finally`; prefer [guarded], which does that
     * for you. Exists so a hot update path can take the lock without constructing a lambda.
     */
    fun enter()

    /** Release the lock acquired by [enter]. */
    fun exit()
}

/**
 * Run [block] holding this lock, without allocating.
 *
 * [Mutex.withLock] is a non-inline interface method taking a function, so every call
 * constructs a capturing lambda. On JVM the JIT scalar-replaces that lambda only while the
 * call site stays monomorphic; once a process has used both [NoopMutex] and [PlatformMutex]
 * the site is polymorphic, escape analysis gives up, and the allocation is real. It measured
 * 32 B per update under [com.eignex.kumulant.core.Concurrency.Strict] in a full-suite run
 * against 0 B when measured alone, and a production process looks like the former. Kotlin
 * platforms without escape analysis pay it unconditionally.
 *
 * Being `inline`, this constructs nothing at all: the branch resolves to a direct
 * `enter`/`try`/`finally`/`exit` around the inlined body, and to the bare body under
 * [NoopMutex]. The `finally` preserves the release-on-throw behaviour `withLock` gave for
 * free, which several stats rely on for their `require` checks.
 */
internal inline fun <R> Mutex.guarded(block: () -> R): R {
    if (this === NoopMutex) return block()
    // Only [NoopMutex] and [PlatformMutex] exist in this module, and every internal lock comes
    // from `welfordLock` / `serializedLock` / `Concurrency.lock`, so this always holds. It
    // cannot fall back to `withLock` for an exotic implementation, because passing an inline
    // parameter to a non-inline function is not allowed - which is the whole reason the
    // allocation exists in the first place.
    check(this is PlatformMutex) {
        "guarded expects a lock from Concurrency.lock(); got ${this::class.simpleName}"
    }
    enter()
    try {
        return block()
    } finally {
        exit()
    }
}
