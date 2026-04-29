package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.defaultStreamMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Weighted sum snapshot. */
@Serializable
@SerialName("Sum")
data class SumResult(
    val sum: Double
) : Result

/** Weighted sum `Σ value*weight` over the stream. */
class Sum(
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<SumResult> {

    private val value = mode.newDouble(0.0)

    override fun update(
        value: Double,
        timestampNanos: Long,
        weight: Double
    ) {
        this.value.add(value * weight)
    }

    override fun read(timestampNanos: Long) = SumResult(value.load())

    override fun merge(values: SumResult) {
        value.add(values.sum)
    }

    override fun reset() {
        value.store(0.0)
    }

    override fun create(mode: StreamMode?) = Sum(mode ?: this.mode)
}
