package com.eignex.kumulant.stream

import com.eignex.util.concurrent.Mutex as StorageMutex

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
 * Create a platform mutex, or a no-op lock when [concurrent] is false
 * and a single thread owns the guarded state.
 * Do not re-enter the same mutex while holding it; re-entry is unsupported across platforms.
 */
fun Mutex(concurrent: Boolean = true): Mutex = MutexAdapter(StorageMutex(concurrent))

private class MutexAdapter(private val delegate: StorageMutex) : Mutex {
    override fun <R> withLock(block: () -> R): R = delegate.withLock(block)
}
