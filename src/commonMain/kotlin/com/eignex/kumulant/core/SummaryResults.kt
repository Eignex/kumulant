package com.eignex.kumulant.core

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Unweighted event count. */
@Serializable
@SerialName("Count")
data class CountResult(
    val count: Long
) : Result

/** Weighted sum snapshot. */
@Serializable
@SerialName("Sum")
data class SumResult(
    val sum: Double
) : Result

/** Arithmetic mean. */
@Serializable
@SerialName("Mean")
data class MeanResult(
    val mean: Double,
) : Result

/** Weighted mean and accumulated weight. */
@Serializable
@SerialName("WeightedMean")
data class WeightedMeanResult(
    val totalWeights: Double,
    val mean: Double,
) : Result

/** Mean and population variance. */
@Serializable
@SerialName("Variance")
data class VarianceResult(
    val mean: Double,
    val variance: Double
) : Result

/** Weighted mean and variance with [totalWeights] for merge arithmetic. */
@Serializable
@SerialName("WeightedVariance")
data class WeightedVarianceResult(
    override val totalWeights: Double,
    val mean: Double,
    override val variance: Double
) : Result, HasSampleVariance

/** First four central moments (m2..m4) plus mean and total weight. */
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
