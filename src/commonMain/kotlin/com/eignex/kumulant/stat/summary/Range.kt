package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.casMax
import com.eignex.kumulant.stream.casMin
import com.eignex.kumulant.stream.defaultStreamMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Running min/max pair of a stream. */
@Serializable
@SerialName("Range")
data class RangeResult(
    val min: Double,
    val max: Double
) : Result

/**
 * Tracks the minimum and maximum of a stream.
 */
class Range(
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<RangeResult> {

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

    override fun create(mode: StreamMode?) = Range(mode ?: this.mode)
}
