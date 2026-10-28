@file:Suppress("MatchingDeclarationName", "Filename")

package com.eignex.kumulant.stream

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

internal actual class PlatformMutex actual constructor() : Mutex {
    private val lock = ReentrantLock()

    // Assertions detect forbidden re-entry under -ea and are eliminated in ordinary builds.
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
