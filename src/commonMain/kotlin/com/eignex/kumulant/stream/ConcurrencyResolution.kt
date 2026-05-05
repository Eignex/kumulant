package com.eignex.kumulant.stream

import com.eignex.kumulant.core.Concurrency

/**
 * Helpers that translate a user-facing [Concurrency] level into the internal cell
 * encoding ([StreamMode]) and lock ([StreamLock]) that honor it for a given stat
 * category.
 *
 * The categories mirror the design plan:
 * - **Additive** (`Sum`, `Count`): single atomic add, no coupling.
 * - **Monotonic** (`Min`, `Max`): single-cell CAS loop, naturally correct.
 * - **Welford-coupled** (`Mean`, `Variance`, `Moments`): multi-cell recurrence;
 *   needs a lock under Strict/HighWrite (cells then drop atomic overhead).
 * - **Self-serialized** sketches (`TDigest`, `SpaceSaving`, `ReservoirHistogram`,
 *   etc.): always under a lock when concurrent; cells are plain `SerialMode`.
 */

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

internal fun Concurrency.welfordLock(): StreamLock = when (this) {
    Concurrency.Strict, Concurrency.HighWrite -> PlatformStreamLock()
    else -> NoopStreamLock
}

internal fun Concurrency.serializedLock(): StreamLock = when (this) {
    Concurrency.None -> NoopStreamLock
    else -> PlatformStreamLock()
}
