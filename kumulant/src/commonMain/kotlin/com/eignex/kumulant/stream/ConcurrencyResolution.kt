package com.eignex.kumulant.stream

import com.eignex.kumulant.core.Concurrency
import com.eignex.util.concurrent.AdderCounters
import com.eignex.util.concurrent.AtomicCells
import com.eignex.util.concurrent.CellMode
import com.eignex.util.concurrent.CounterMode
import com.eignex.util.concurrent.SerialCells
import com.eignex.util.concurrent.Mutex as StorageMutex

// Resolve each stat's concurrency mode according to its state dependencies:
// - Additive: independent atomic adds; JVM HighWrite uses striped scalar counters.
// - Monotonic: CAS cells preserve extrema, reference publication and per-slot array updates.
// - Welford-coupled: Relaxed uses atomic cells; Strict/HighWrite lock plain cells.
// - Self-serialised: every concurrent mode locks the body around plain cells.

internal fun Concurrency.additiveMode(): CounterMode = when (this) {
    Concurrency.None -> SerialCells
    Concurrency.Relaxed, Concurrency.Strict -> AtomicCells
    Concurrency.HighWrite -> AdderCounters
}

internal fun Concurrency.monotonicMode(): CellMode = when (this) {
    Concurrency.None -> SerialCells
    else -> AtomicCells
}

internal fun Concurrency.welfordMode(): CellMode = when (this) {
    Concurrency.Relaxed -> AtomicCells
    else -> SerialCells
}

internal fun Concurrency.welfordLock(): StorageMutex =
    StorageMutex(concurrent = this == Concurrency.Strict || this == Concurrency.HighWrite)

internal fun Concurrency.serializedLock(): StorageMutex = StorageMutex(concurrent = this != Concurrency.None)

// First-writer election requires CAS, which striped counters cannot provide.
internal fun Concurrency.firstWriterMode(): CellMode = monotonicMode()
