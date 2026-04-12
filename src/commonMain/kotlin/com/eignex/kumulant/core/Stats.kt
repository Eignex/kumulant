package com.eignex.kumulant.core

import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.currentTimeNanos

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
    fun read(timestampNanos: Long = currentTimeNanos()): R

    fun create(mode: StreamMode? = null): Stat<R>
}

interface SeriesStat<R : Result> : Stat<R> {
    fun update(value: Double, weight: Double = 1.0) =
        update(value, currentTimeNanos(), weight)

    fun update(value: Double, timestampNanos: Long, weight: Double = 1.0)

    override fun create(mode: StreamMode?): SeriesStat<R>
}

interface PairedStat<R : Result> : Stat<R> {
    fun update(x: Double, y: Double, weight: Double = 1.0) =
        update(x, y, currentTimeNanos(), weight)

    fun update(x: Double, y: Double, timestampNanos: Long, weight: Double = 1.0)

    override fun create(mode: StreamMode?): PairedStat<R>
}

interface VectorStat<R : Result> : Stat<R> {
    fun update(vector: DoubleArray, weight: Double = 1.0) =
        update(vector, currentTimeNanos(), weight)

    fun update(vector: DoubleArray, timestampNanos: Long, weight: Double = 1.0)

    override fun create(mode: StreamMode?): VectorStat<R>
}
