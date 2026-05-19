package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.casMin
import com.eignex.kumulant.stream.monotonicMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Running minimum of a stream. */
@Serializable
@SerialName("MinResult")
data class MinResult(
    /** Running minimum value. */
    val min: Double,
) : Result

/**
 * Tracks the minimum value seen across a stream.
 *
 * # Concurrency
 *
 * Single-cell CAS-min loop — exact under every [Concurrency] level.
 * The CAS retry naturally serialises racing writers without a lock.
 */
class MinStat(
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<MinResult> {

    private val mode = concurrency.monotonicMode()
    private val value = mode.newDouble(Double.POSITIVE_INFINITY)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        casMin(this.value, value)
    }

    override fun merge(values: MinResult) {
        casMin(value, values.min)
    }

    override fun reset() {
        value.store(Double.POSITIVE_INFINITY)
    }

    override fun read(timestampNanos: Long) = MinResult(value.load())

    override fun create(concurrency: Concurrency?) = MinStat(concurrency ?: this.concurrency)
}
