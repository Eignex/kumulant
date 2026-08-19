@file:Suppress("MatchingDeclarationName", "Filename")

package com.eignex.kumulant.stream

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

internal actual class PlatformMutex actual constructor() : Mutex {
    private val lock = ReentrantLock()

    // ReentrantLock would happily grant a second acquisition to the same thread, so the JVM cannot
    // discover a re-entry the way an error-checking posix mutex does. `assert` closes that gap where
    // it is free: enabled under -ea, which Gradle turns on for tests, and a JIT-eliminated branch
    // otherwise. It answers on the target the tests run on first, before a native build hangs.
    private fun requireNotHeld() {
        assert(!lock.isHeldByCurrentThread) {
            "this thread already holds this lock; PlatformMutex is not reentrant"
        }
    }

    actual override fun <R> withLock(block: () -> R): R {
        requireNotHeld()
        return lock.withLock(block)
    }

    actual fun enter() {
        requireNotHeld()
        lock.lock()
    }

    actual fun exit() = lock.unlock()
}
