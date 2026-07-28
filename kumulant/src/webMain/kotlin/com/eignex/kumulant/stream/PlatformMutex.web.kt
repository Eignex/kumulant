@file:Suppress("MatchingDeclarationName", "Filename")

package com.eignex.kumulant.stream

internal actual class PlatformMutex actual constructor() : Mutex {
    actual override fun <R> withLock(block: () -> R): R = block()

    // JS and current Kotlin/Wasm are single-threaded with no shared heap, so there is
    // nothing to acquire. See the note on Mutex for why that is safe today.
    actual fun enter() = Unit

    actual fun exit() = Unit
}
