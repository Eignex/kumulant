package com.eignex.kumulant.core

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
@SerialName("Count")
data class CountResult(
    val count: Long
) : Result

@Serializable
@SerialName("Sum")
data class SumResult(
    val sum: Double
) : Result

@Serializable
@SerialName("Mean")
data class MeanResult(
    val mean: Double,
) : Result

@Serializable
@SerialName("WeightedMean")
data class WeightedMeanResult(
    val totalWeights: Double,
    val mean: Double,
) : Result

@Serializable
@SerialName("Variance")
data class VarianceResult(
    val mean: Double,
    val variance: Double
) : Result

@Serializable
@SerialName("WeightedVariance")
data class WeightedVarianceResult(
    override val totalWeights: Double,
    val mean: Double,
    override val variance: Double
) : Result, HasSampleVariance

@Serializable
@SerialName("Moments")
data class MomentsResult(
    override val totalWeights: Double,
    val mean: Double,
    override val m2: Double,
    override val m3: Double,
    override val m4: Double
) : Result, HasSampleVariance, HasShapeMoments {
    override val sst: Double get() = m2
}
