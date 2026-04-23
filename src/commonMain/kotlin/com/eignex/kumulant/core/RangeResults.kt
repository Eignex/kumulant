package com.eignex.kumulant.core

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
@SerialName("Min")
data class MinResult(
    val min: Double
) : Result

@Serializable
@SerialName("Max")
data class MaxResult(
    val max: Double
) : Result

@Serializable
@SerialName("Range")
data class RangeResult(
    val min: Double,
    val max: Double
) : Result
