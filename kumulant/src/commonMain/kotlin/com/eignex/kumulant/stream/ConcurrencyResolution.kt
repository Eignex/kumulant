package com.eignex.kumulant.stream

import com.eignex.kumulant.core.Concurrency

/**
 * Helpers that translate a user-facing [Concurrency] level into the internal cell
 * encoding ([StreamMode]) and lock ([Mutex]) that honor it for a given stat
 * category.
 *
 * Stat categories:
 * - Additive ([SumStat][com.eignex.kumulant.stat.summary.SumStat],
 *   [CountStat][com.eignex.kumulant.stat.summary.CountStat]): single atomic add, no coupling.
 * - Monotonic ([MinStat][com.eignex.kumulant.stat.summary.MinStat],
 *   [MaxStat][com.eignex.kumulant.stat.summary.MaxStat]): single-cell CAS loop, naturally correct.
 * - Welford-coupled ([MeanStat][com.eignex.kumulant.stat.summary.MeanStat],
 *   [VarianceStat][com.eignex.kumulant.stat.summary.VarianceStat],
 *   [MomentsStat][com.eignex.kumulant.stat.summary.MomentsStat]): multi-cell recurrence;
 *   needs a lock under Strict/HighWrite (cells then drop atomic overhead).
 * - Self-serialized sketches ([TDigestStat][com.eignex.kumulant.stat.quantile.TDigestStat],
 *   [SpaceSavingStat][com.eignex.kumulant.stat.sketch.SpaceSavingStat],
 *   [ReservoirHistogramStat][com.eignex.kumulant.stat.quantile.ReservoirHistogramStat], etc.):
 *   always under a lock when concurrent; cells are plain [SerialMode].
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

internal fun Concurrency.welfordLock(): Mutex = when (this) {
    Concurrency.Strict, Concurrency.HighWrite -> PlatformMutex()
    else -> NoopMutex
}

/**
 * The lock a stat takes to serialise a multi-cell update.
 *
 * Delegates to [Concurrency.lock] so the mapping lives in one place; the separate name is kept because it
 * says why the lock is being taken.
 */
internal fun Concurrency.serializedLock(): Mutex = lock()

/** Cell mode for a single first-writer-wins field that needs CAS (e.g. a stat's
 *  lazily-initialised start timestamp). HighWrite's striped adders don't support
 *  CAS, so this returns [AtomicMode] for every concurrent level; write contention
 *  on a cell that's only ever written once is irrelevant anyway. */
internal fun Concurrency.firstWriterMode(): StreamMode = when (this) {
    Concurrency.None -> SerialMode
    else -> AtomicMode
}
