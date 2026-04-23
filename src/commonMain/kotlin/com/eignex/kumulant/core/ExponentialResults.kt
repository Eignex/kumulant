package com.eignex.kumulant.core

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.sqrt

@Serializable
@SerialName("DecayingSum")
data class DecayingSumResult(
    val sum: Double,
    val timestampNanos: Long,
) : Result

@Serializable
@SerialName("DecayingMean")
data class DecayingMeanResult(
    val mean: Double,
    /** Effective weight of observations still contributing (decays with time). */
    val totalWeights: Double,
    val timestampNanos: Long,
) : Result

@Serializable
@SerialName("DecayingVariance")
data class DecayingVarianceResult(
    val mean: Double,
    val variance: Double,
    /** Effective weight of observations still contributing (decays with time). */
    val totalWeights: Double,
    val timestampNanos: Long,
) : Result {
    val stdDev: Double get() = sqrt(variance)
}
