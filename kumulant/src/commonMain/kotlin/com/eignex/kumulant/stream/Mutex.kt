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
 * Create a no-op lock for [Concurrency.None], otherwise a platform mutex.
 * Do not re-enter the same mutex while holding it; re-entry is unsupported across platforms.
 */
fun Concurrency.lock(): Mutex = when (this) {
    Concurrency.None -> NoopMutex
    else -> PlatformMutex()
}

internal object NoopMutex : Mutex {
    override fun <R> withLock(block: () -> R): R = block()
}

// JVM uses ReentrantLock; POSIX uses a GC-cleaned, error-checking pthread mutex.
// Treat both as non-reentrant: POSIX rejects re-entry even though JVM permits it.
internal expect class PlatformMutex() : Mutex {
    override fun <R> withLock(block: () -> R): R

    // Hot paths use enter/exit to avoid a lambda allocation; guarded ensures release on exceptions.
    fun enter()

    fun exit()
}

// Inlining avoids a capturing lambda on hot update paths, including targets without escape analysis.
// The finally block must release the lock when validation inside the body throws.
internal inline fun <R> Mutex.guarded(block: () -> R): R {
    if (this === NoopMutex) return block()
    // All internal lock factories return one of these two implementations.
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
