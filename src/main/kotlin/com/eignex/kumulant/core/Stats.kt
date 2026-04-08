package com.eignex.kumulant.core

import com.eignex.kumulant.concurrent.StreamMode

/**
 * The base interface for all statistical accumulators.
 *
 * @param R The type of the result object produced by this statistic.
 */
interface Stat<R : Result> {
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
    fun read(timestampNanos: Long = System.nanoTime()): R

    fun copy(mode: StreamMode? = null, name: String? = null): Stat<R>

    val name: String?
}

interface SeriesStat<R : Result> : Stat<R> {
    fun update(value: Double, weight: Double = 1.0) =
        update(value, System.nanoTime(), weight)

    fun update(value: Double, timestampNanos: Long, weight: Double = 1.0)

    override fun copy(mode: StreamMode?, name: String?): SeriesStat<R>
}

interface PairedStat<R : Result> : Stat<R> {
    fun update(x: Double, y: Double, weight: Double = 1.0) =
        update(x, y, System.nanoTime(), weight)

    fun update(x: Double, y: Double, timestampNanos: Long, weight: Double = 1.0)

    override fun copy(mode: StreamMode?, name: String?): PairedStat<R>
}

interface VectorStat<R : Result> : Stat<R> {
    fun update(vector: DoubleArray, weight: Double = 1.0) =
        update(vector, System.nanoTime(), weight)

    fun update(vector: DoubleArray, timestampNanos: Long, weight: Double = 1.0)

    override fun copy(mode: StreamMode?, name: String?): VectorStat<R>
}
