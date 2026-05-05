package com.eignex.kumulant.stream

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

actual class PlatformStreamLock actual constructor() : StreamLock {
    private val lock = ReentrantLock()
    actual override fun <R> withLock(block: () -> R): R = lock.withLock(block)
}
