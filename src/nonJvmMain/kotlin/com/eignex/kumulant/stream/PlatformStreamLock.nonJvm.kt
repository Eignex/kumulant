@file:OptIn(ExperimentalAtomicApi::class)

package com.eignex.kumulant.stream

import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi

/**
 * CAS spin-mutex for non-JVM targets. On single-threaded runtimes (JS, Wasm) the
 * acquire CAS always succeeds on the first try, so this is effectively a noop with
 * one atomic op of overhead. On Native (which has real threads via Workers), it
 * spins under contention; acceptable for short critical sections.
 */
actual class PlatformStreamLock actual constructor() : StreamLock {
    private val flag = AtomicLong(0L)

    actual override fun <R> withLock(block: () -> R): R {
        while (!flag.compareAndSet(0L, 1L)) {
            // spin
        }
        try {
            return block()
        } finally {
            flag.store(0L)
        }
    }
}
