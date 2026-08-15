@file:Suppress("MatchingDeclarationName", "Filename")

package com.eignex.kumulant.stream

/** `Thread.onSpinWait()`, which emits the architecture's pause instruction. See the `expect`. */
internal actual fun spinHint() {
    Thread.onSpinWait()
}
