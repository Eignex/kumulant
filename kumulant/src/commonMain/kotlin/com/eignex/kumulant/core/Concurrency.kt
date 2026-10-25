package com.eignex.kumulant.core

/**
 * Thread-safety mode for statistical accumulators. Each implementation documents
 * how the mode maps to its cells and locks.
 *
 * Bare stats default to [None]. A [com.eignex.kumulant.schema.runtime.StatSchema]
 * can be materialised with a mode that applies to all of its stats.
 *
 * - [None] requires a single-threaded owner.
 * - [Relaxed] uses atomic cells for additive and monotonic stats, permits drift for
 *   coupled recurrences, and locks structures that require serialised mutation.
 * - [Strict] protects coupled recurrences with a lock.
 * - [HighWrite] uses striped adders for additive JVM stats and behaves like [Strict]
 *   for coupled state and on other platforms.
 *
 * Atomic updates to individual cells do not guarantee an atomic snapshot of several
 * cells. Check each stat's concurrency documentation when snapshot consistency matters.
 * Floating-point results can vary with update order even when updates are serialised.
 *
 * @sample com.eignex.kumulant.samples.perStatConcurrency
 */
enum class Concurrency {

    /**
     * Single-threaded access with no synchronisation. Sharing a stat between threads
     * under this mode can corrupt state or throw.
     */
    None,

    /**
     * Concurrent updates with implementation-specific consistency.
     * Independent cells use atomic updates. Coupled recurrences such as mean and variance
     * can drift under contention; no numerical error bound is guaranteed. Sketches that
     * require serialised mutation still lock.
     */
    Relaxed,

    /**
     * Concurrent access with locks where coupled state requires them.
     * Independent atomic cells remain lock-free; a multi-cell read is coherent only when
     * the implementation holds a shared lock across reads and writes.
     */
    Strict,

    /**
     * Striped adders for additive JVM stats, trading cheaper contended updates for a
     * read that sums the stripes without an atomic snapshot. Behaves like [Strict] on
     * non-JVM platforms and for stats whose state cannot be striped.
     */
    HighWrite,
}

internal fun Iterable<Concurrency>.combinedConcurrency(empty: Concurrency = Concurrency.None): Concurrency =
    reduceOrNull { left, right ->
        when {
            left == right -> left
            left == Concurrency.None || right == Concurrency.None -> Concurrency.None
            left == Concurrency.Relaxed || right == Concurrency.Relaxed -> Concurrency.Relaxed
            else -> Concurrency.Strict
        }
    } ?: empty
