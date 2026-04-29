package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.casMin
import com.eignex.kumulant.stream.defaultStreamMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Running minimum of a stream. */
@Serializable
@SerialName("Min")
data class MinResult(
    val min: Double
) : Result

/** Tracks the minimum of a stream. */
class Min(
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<MinResult> {

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

    override fun create(mode: StreamMode?) = Min(mode ?: this.mode)
}
