package com.eignex.kumulant.stat.score

import com.eignex.kumulant.core.HasObservationCount
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Weighted classification accuracy and the total weight of admitted label pairs. */
@Serializable
@SerialName("AccuracyResult")
data class AccuracyResult(
    /** Weighted fraction of matching predicted and true labels; zero before any observation. */
    val accuracy: Double,
    override val totalWeights: Double,
) : HasObservationCount
