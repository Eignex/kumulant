package com.eignex.kumulant.core

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

/** Exponentially time-decayed rate snapshot. */
@Serializable
@SerialName("DecayingRate")
data class DecayingRateResult(
    override val rate: Double,
    val timestampNanos: Long,
) : Result, HasRate
