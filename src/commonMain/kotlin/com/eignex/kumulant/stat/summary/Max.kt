package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.casMax
import com.eignex.kumulant.stream.defaultStreamMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Running maximum of a stream. */
@Serializable
@SerialName("Max")
data class MaxResult(
    val max: Double
) : Result

/** Tracks the maximum of a stream. */
class Max(
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<MaxResult> {

    private val value = mode.newDouble(Double.NEGATIVE_INFINITY)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        casMax(this.value, value)
    }

    override fun merge(values: MaxResult) {
        casMax(value, values.max)
    }

    override fun reset() {
        value.store(Double.NEGATIVE_INFINITY)
    }

    override fun read(timestampNanos: Long) = MaxResult(value.load())

    override fun create(mode: StreamMode?) = Max(mode ?: this.mode)
}
