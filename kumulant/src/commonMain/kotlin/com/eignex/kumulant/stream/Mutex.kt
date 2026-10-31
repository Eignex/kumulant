package com.eignex.kumulant.stream

import com.eignex.kumulant.core.Concurrency

/**
 * Mutual-exclusion lock used by stats whose update logic spans multiple cells or
 * multiple steps and therefore can't be made elementwise atomic. Create one with
 * [Mutex].
 */
interface Mutex {
    /** Run [block] holding the lock and return its result. */
    fun <R> withLock(block: () -> R): R
}

/**
 * Create a platform mutex, or a no-op lock with zero overhead when [concurrent] is false
 * and a single thread owns the guarded state.
 * Do not re-enter the same mutex while holding it; re-entry is unsupported across platforms.
 */
fun Mutex(concurrent: Boolean = true): Mutex = if (concurrent) PlatformMutex() else NoopMutex

internal fun Concurrency.lock(): Mutex = Mutex(concurrent = this != Concurrency.None)

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
        "guarded expects a lock from Mutex(); got ${this::class.simpleName}"
    }
    enter()
    try {
        return block()
    } finally {
        exit()
    }
}
