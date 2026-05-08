@file:Suppress("MatchingDeclarationName", "Filename")

package com.eignex.kumulant.stream

actual class PlatformStreamLock actual constructor() : StreamLock {
    actual override fun <R> withLock(block: () -> R): R = block()
}
