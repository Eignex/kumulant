package com.eignex.kumulant.core

import com.eignex.kumulant.stream.currentTimeNanos

/**
 * The base interface for all statistical accumulators.
 *
 * @param R The type of the result object produced by this statistic.
 */
interface Stat<R : Result> {
    /**
     * The thread-safety contract this stat was constructed with. Each stat picks the
     * cell-encoding and lock strategy honoring this contract for its mathematical
     * structure:
     *
     * - [Concurrency.None]: single-threaded; no synchronisation. Cheapest path.
     * - [Concurrency.Relaxed]: lock-free best-effort. Multi-cell stats (Welford-style
     *   `Mean`, `Variance`, `Moments`) may drift under contention but never throw.
     * - [Concurrency.Strict]: serialised when needed for full correctness across
     *   coupled cells. Sketches always self-serialise; Welford stats lock per update.
     * - [Concurrency.HighWrite]: optimised for many concurrent writers; JVM uses
     *   striped adders for naively additive stats.
     */
    val concurrency: Concurrency

    /**
     * Merge stat results from another accumulator into this.
     */
    fun merge(values: R)

    /**
     * Reset stats to an initial state.
     */
    fun reset()

    /**
     * Computes and returns the current state of the statistic.
     */
    fun read(timestampNanos: Long = currentTimeNanos()): R

    /** Returns a fresh accumulator with the same configuration, optionally overriding the [Concurrency]. */
    fun create(concurrency: Concurrency? = null): Stat<R>
}

/** Accumulator over a single scalar time series. */
interface SeriesStat<R : Result> : Stat<R> {
    /** Record an observation with the given [weight], stamped at the current time. */
    fun update(value: Double, weight: Double = 1.0) =
        update(value, currentTimeNanos(), weight)

    /** Record an observation at [timestampNanos] with the given [weight]. */
    fun update(value: Double, timestampNanos: Long, weight: Double = 1.0)

    override fun create(concurrency: Concurrency?): SeriesStat<R>
}

/**
 * Accumulator over a stream of discrete `Long` values — covers both opaque keys
 * (cardinality, heavy hitters, Bloom filters) and integer-valued measurements
 * (Poisson counts, time deltas, integer histograms).
 */
interface DiscreteStat<R : Result> : Stat<R> {
    /** Record an observation with the given [weight], stamped at the current time. */
    fun update(value: Long, weight: Double = 1.0) =
        update(value, currentTimeNanos(), weight)

    /** Record an observation at [timestampNanos] with the given [weight]. */
    fun update(value: Long, timestampNanos: Long, weight: Double = 1.0)

    override fun create(concurrency: Concurrency?): DiscreteStat<R>
}

/** Accumulator over paired (x, y) observations such as a regression. */
interface PairedStat<R : Result> : Stat<R> {
    /** Record an (x, y) observation with the given [weight] at the current time. */
    fun update(x: Double, y: Double, weight: Double = 1.0) =
        update(x, y, currentTimeNanos(), weight)

    /** Record an (x, y) observation at [timestampNanos] with the given [weight]. */
    fun update(x: Double, y: Double, timestampNanos: Long, weight: Double = 1.0)

    override fun create(concurrency: Concurrency?): PairedStat<R>
}

/** Accumulator over fixed-dimensional vector observations. */
interface VectorStat<R : Result> : Stat<R> {
    /** Record a [vector] observation with the given [weight] at the current time. */
    fun update(vector: DoubleArray, weight: Double = 1.0) =
        update(vector, currentTimeNanos(), weight)

    /** Record a [vector] observation at [timestampNanos] with the given [weight]. */
    fun update(vector: DoubleArray, timestampNanos: Long, weight: Double = 1.0)

    override fun create(concurrency: Concurrency?): VectorStat<R>
}
