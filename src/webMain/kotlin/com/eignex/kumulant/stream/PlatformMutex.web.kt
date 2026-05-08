@file:Suppress("MatchingDeclarationName", "Filename")

package com.eignex.kumulant.stream

actual class PlatformMutex actual constructor() : Mutex {
    actual override fun <R> withLock(block: () -> R): R = block()
}
