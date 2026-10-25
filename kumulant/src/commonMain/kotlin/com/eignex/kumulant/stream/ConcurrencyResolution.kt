package com.eignex.kumulant.stream

import com.eignex.kumulant.core.Concurrency

// Resolve each stat's concurrency mode according to its state dependencies:
// - Additive: independent atomic adds; JVM HighWrite uses striped adders.
// - Monotonic: CAS loops preserve extrema without a lock.
// - Welford-coupled: Relaxed uses atomic cells; Strict/HighWrite lock plain cells.
// - Self-serialised: every concurrent mode locks the body around plain cells.

internal fun Concurrency.additiveMode(): StreamMode = when (this) {
    Concurrency.None -> SerialMode
    Concurrency.Relaxed, Concurrency.Strict -> AtomicMode
    Concurrency.HighWrite -> highWriteMode
}

internal fun Concurrency.monotonicMode(): StreamMode = when (this) {
    Concurrency.None -> SerialMode
    else -> AtomicMode
}

internal fun Concurrency.welfordMode(): StreamMode = when (this) {
    Concurrency.Relaxed -> AtomicMode
    else -> SerialMode
}

internal fun Concurrency.welfordLock(): Mutex = when (this) {
    Concurrency.Strict, Concurrency.HighWrite -> PlatformMutex()
    else -> NoopMutex
}

internal fun Concurrency.serializedLock(): Mutex = lock()

// First-writer election requires CAS, which striped adders cannot provide.
internal fun Concurrency.firstWriterMode(): StreamMode = when (this) {
    Concurrency.None -> SerialMode
    else -> AtomicMode
}
