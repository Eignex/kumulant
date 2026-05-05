package com.eignex.kumulant.core

/**
 * User-facing concurrency contract for stats. Each stat translates the chosen level
 * into a cell-encoding and lock strategy that honors it for that stat's mathematical
 * structure.
 *
 * - [None]: single-threaded; no synchronisation. Cheapest path.
 * - [Relaxed]: multi-threaded; coupled-state stats may drift, but no exceptions.
 * - [Strict]: multi-threaded; serialised when needed for full correctness.
 * - [HighWrite]: multi-threaded write-heavy. JVM uses striped adders for naively
 *   additive stats; on other platforms behaves like [Strict].
 */
enum class Concurrency {
    None,
    Relaxed,
    Strict,
    HighWrite,
}

/**
 * Global default [Concurrency] used by stat constructors when none is passed. Mutable
 * via [withConcurrency] or direct assignment. Not thread-isolated: in concurrent
 * contexts where different threads need different defaults, pass `concurrency =`
 * explicitly to each stat constructor instead of relying on this global.
 */
var defaultConcurrency: Concurrency = Concurrency.None

/** Temporarily overrides [defaultConcurrency] for the duration of the block. */
inline fun <T> withConcurrency(concurrency: Concurrency, block: () -> T): T {
    val previous = defaultConcurrency
    defaultConcurrency = concurrency
    return try {
        block()
    } finally {
        defaultConcurrency = previous
    }
}
