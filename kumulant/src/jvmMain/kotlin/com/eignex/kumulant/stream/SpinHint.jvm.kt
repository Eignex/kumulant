@file:Suppress("MatchingDeclarationName", "Filename")

package com.eignex.kumulant.stream

/**
 * CPU hint used inside busy-wait loops. `Thread.onSpinWait()` lets the JVM
 * issue the architecture's pause/yield instruction so the spin doesn't
 * monopolise its core's pipeline; on single-threaded targets (JS, Wasm)
 * this is a no-op.
 */
internal actual fun spinHint() {
    Thread.onSpinWait()
}
