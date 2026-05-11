package com.eignex.kumulant.schema

import com.eignex.kumulant.stat.decay.DecayWeighting
import com.eignex.kumulant.stat.decay.DecayingMeanResult
import com.eignex.kumulant.stat.decay.DecayingSumResult
import com.eignex.kumulant.stat.decay.DecayingVarianceResult
import com.eignex.kumulant.stat.rate.DecayingRateResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.time.Duration.Companion.milliseconds

/**
 * Wire-friendly counterpart of [DecayWeighting]. The two strategies are split
 * by field type rather than discriminated union so each decay-stat spec can
 * statically constrain itself to the right strategy (e.g. [DecayingSum]
 * only accepts [HalfLife]).
 *
 * Wall-clock durations travel as `Long` milliseconds rather than
 * `kotlin.time.Duration` to avoid the experimental `Duration` serializer and
 * keep the wire compact.
 */
@Serializable
sealed interface DecayWeightingSpec

@Serializable
@SerialName("HalfLife")
data class HalfLife(val durationMillis: Long) : DecayWeightingSpec {
    fun toDecayWeighting(): DecayWeighting.HalfLife = DecayWeighting.HalfLife(durationMillis.milliseconds)
}

@Serializable
@SerialName("Alpha")
data class Alpha(val alpha: Double) : DecayWeightingSpec {
    fun toDecayWeighting(): DecayWeighting.Alpha = DecayWeighting.Alpha(alpha)
}

@Serializable
@SerialName("DecayingSum")
data class DecayingSum(val weighting: HalfLife) : SeriesStatSpec<DecayingSumResult>

@Serializable
@SerialName("DecayingMean")
data class DecayingMean(val weighting: HalfLife) : SeriesStatSpec<DecayingMeanResult>

@Serializable
@SerialName("DecayingVariance")
data class DecayingVariance(val weighting: HalfLife) : SeriesStatSpec<DecayingVarianceResult>

@Serializable
@SerialName("EwmaMean")
data class EwmaMean(val weighting: Alpha) : SeriesStatSpec<WeightedMeanResult>

@Serializable
@SerialName("EwmaVariance")
data class EwmaVariance(val weighting: Alpha) : SeriesStatSpec<WeightedVarianceResult>

@Serializable
@SerialName("DecayingRate")
data class DecayingRate(val halfLifeMillis: Long) : SeriesStatSpec<DecayingRateResult>
