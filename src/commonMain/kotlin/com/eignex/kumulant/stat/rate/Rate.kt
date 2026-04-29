package com.eignex.kumulant.stat.rate

import com.eignex.kumulant.core.HasRate
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.defaultStreamMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Cumulative rate: [totalValue] accumulated from [startTimestampNanos] to [timestampNanos]. */
@Serializable
@SerialName("Rate")
data class RateResult(
    val startTimestampNanos: Long,
    val totalValue: Double,
    val timestampNanos: Long
) : Result, HasRate {
    override val rate: Double
        get() {
            val durationSeconds = (timestampNanos - startTimestampNanos) / 1e9
            val safeDuration =
                if (durationSeconds <= 0.0) 1e-9 else durationSeconds
            return totalValue / safeDuration
        }
}

/**
 * Cumulative rate: total accumulated value divided by elapsed time since the
 * first update. Useful for measuring throughput over the lifetime of a stream.
 *
 * For time-decaying rates that weight recent observations more heavily,
 * see [DecayingRate]. Use [withValue][com.eignex.kumulant.operation.withValue] to count each update as 1.
 */
class Rate(
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<RateResult> {

    private val totalValues = mode.newDouble(0.0)
    private val startTimestampNanos = mode.newLong(Long.MIN_VALUE)

    override fun update(
        value: Double,
        timestampNanos: Long,
        weight: Double
    ) {
        if (startTimestampNanos.load() == Long.MIN_VALUE) {
            startTimestampNanos.store(timestampNanos)
        }
        totalValues.add(value * weight)
    }

    override fun create(mode: StreamMode?) = Rate(mode ?: this.mode)

    override fun read(timestampNanos: Long): RateResult {
        val start = if (startTimestampNanos.load() == Long.MIN_VALUE) {
            timestampNanos
        } else {
            startTimestampNanos.load()
        }
        return RateResult(
            startTimestampNanos = start,
            totalValue = totalValues.load(),
            timestampNanos = timestampNanos
        )
    }

    override fun merge(values: RateResult) {
        if (values.totalValue == 0.0) return

        totalValues.add(values.totalValue)

        val currentStart = startTimestampNanos.load()
        if (currentStart == Long.MIN_VALUE || values.startTimestampNanos < currentStart) {
            startTimestampNanos.store(values.startTimestampNanos)
        }
    }

    override fun reset() {
        startTimestampNanos.store(Long.MIN_VALUE)
        totalValues.store(0.0)
    }
}
