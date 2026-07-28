@file:Suppress("MatchingDeclarationName", "Filename")

package com.eignex.kumulant.stream

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

internal actual class PlatformMutex actual constructor() : Mutex {
    @Suppress("UnusedPrivateProperty") // detekt misses the kotlin.concurrent.withLock extension call
    private val lock = ReentrantLock()
    actual override fun <R> withLock(block: () -> R): R = lock.withLock(block)

    actual fun enter() = lock.lock()

    actual fun exit() = lock.unlock()
}
