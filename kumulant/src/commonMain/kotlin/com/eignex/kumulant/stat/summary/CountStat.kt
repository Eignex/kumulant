package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.operation.withValue
import com.eignex.kumulant.operation.withWeight
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Unweighted event count. */
@Serializable
@SerialName("CountResult")
data class CountResult(
    /** Number of observations seen. */
    val count: Long,
) : Result

/**
 * Observation count: each update contributes 1 regardless of supplied value and weight.
 *
 * # Concurrency
 *
 * Inherits [SumStat]'s single-atomic-add update path — exact under every
 * [Concurrency] level.
 */
class CountStat(concurrency: Concurrency = Concurrency.None) :
    SeriesStat<SumResult> by SumStat(concurrency).withWeight(1.0).withValue(1.0)
