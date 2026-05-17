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
 *
 * Bare-stat construction defaults to [None]. To configure a coherent bag of stats
 * with one contract, declare them inside a `StatSchema(concurrency = ...)` and the
 * schema propagates the choice to every registered stat at delegate registration.
 */
enum class Concurrency {
    None,
    Relaxed,
    Strict,
    HighWrite,
}
