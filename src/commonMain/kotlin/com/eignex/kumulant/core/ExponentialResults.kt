package com.eignex.kumulant.core

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.sqrt

/** Snapshot of an exponentially time-decayed sum at [timestampNanos]. */
@Serializable
@SerialName("DecayingSum")
data class DecayingSumResult(
    val sum: Double,
    val timestampNanos: Long,
) : Result

/** Snapshot of an exponentially time-decayed weighted mean at [timestampNanos]. */
@Serializable
@SerialName("DecayingMean")
data class DecayingMeanResult(
    val mean: Double,
    /** Effective weight of observations still contributing (decays with time). */
    val totalWeights: Double,
    val timestampNanos: Long,
) : Result

/** Snapshot of an exponentially time-decayed weighted variance at [timestampNanos]. */
@Serializable
@SerialName("DecayingVariance")
data class DecayingVarianceResult(
    val mean: Double,
    val variance: Double,
    /** Effective weight of observations still contributing (decays with time). */
    val totalWeights: Double,
    val timestampNanos: Long,
) : Result {
    /** Square root of [variance]. */
    val stdDev: Double get() = sqrt(variance)
}
