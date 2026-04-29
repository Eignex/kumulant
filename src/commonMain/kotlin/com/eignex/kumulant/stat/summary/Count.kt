package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.operation.withValue
import com.eignex.kumulant.operation.withWeight
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.defaultStreamMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Unweighted event count. */
@Serializable
@SerialName("Count")
data class CountResult(
    val count: Long
) : Result

/** Observation count: each update contributes 1 regardless of supplied value and weight. */
class Count(val mode: StreamMode = defaultStreamMode) :
    SeriesStat<SumResult> by Sum(mode).withWeight(1.0).withValue(1.0)
