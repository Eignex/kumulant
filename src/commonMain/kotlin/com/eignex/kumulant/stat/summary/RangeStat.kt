package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.casMax
import com.eignex.kumulant.stream.casMin
import com.eignex.kumulant.stream.monotonicMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Running min/max pair of a stream. */
@Serializable
@SerialName("RangeResult")
data class RangeResult(
    /** Running minimum value. */
    val min: Double,
    /** Running maximum value. */
    val max: Double,
) : Result

/**
 * Tracks the minimum and maximum of a stream.
 */
class RangeStat(
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<RangeResult> {

    private val mode = concurrency.monotonicMode()
    private val min = mode.newDouble(Double.POSITIVE_INFINITY)
    private val max = mode.newDouble(Double.NEGATIVE_INFINITY)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        casMin(min, value)
        casMax(max, value)
    }

    override fun merge(values: RangeResult) {
        casMin(min, values.min)
        casMax(max, values.max)
    }

    override fun reset() {
        min.store(Double.POSITIVE_INFINITY)
        max.store(Double.NEGATIVE_INFINITY)
    }

    override fun read(timestampNanos: Long) = RangeResult(min.load(), max.load())

    override fun create(concurrency: Concurrency?) = RangeStat(concurrency ?: this.concurrency)
}
