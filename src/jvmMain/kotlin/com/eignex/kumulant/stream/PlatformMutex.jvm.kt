@file:Suppress("MatchingDeclarationName", "Filename")

package com.eignex.kumulant.stream

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

actual class PlatformMutex actual constructor() : Mutex {
    private val lock = ReentrantLock()
    actual override fun <R> withLock(block: () -> R): R = lock.withLock(block)
}
