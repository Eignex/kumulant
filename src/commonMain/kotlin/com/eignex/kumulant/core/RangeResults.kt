package com.eignex.kumulant.core

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Running minimum of a stream. */
@Serializable
@SerialName("Min")
data class MinResult(
    val min: Double
) : Result

/** Running maximum of a stream. */
@Serializable
@SerialName("Max")
data class MaxResult(
    val max: Double
) : Result

/** Running min/max pair of a stream. */
@Serializable
@SerialName("Range")
data class RangeResult(
    val min: Double,
    val max: Double
) : Result
