package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stat.decay.DecayWeighting
import com.eignex.kumulant.stat.decay.DecayingMeanStat
import com.eignex.kumulant.stat.decay.DecayingMeanResult
import com.eignex.kumulant.stat.decay.DecayingSumStat
import com.eignex.kumulant.stat.decay.DecayingSumResult
import com.eignex.kumulant.stat.decay.DecayingVarianceStat
import com.eignex.kumulant.stat.decay.DecayingVarianceResult
import com.eignex.kumulant.stat.decay.EwmaMeanStat
import com.eignex.kumulant.stat.decay.EwmaVarianceStat
import com.eignex.kumulant.stat.rate.DecayingRateStat
import com.eignex.kumulant.stat.rate.DecayingRateResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.time.Duration.Companion.milliseconds

/**
 * Wire-friendly counterpart of [DecayWeighting]. The two strategies are split
 * by field type rather than discriminated union so each decay-stat config can
 * statically constrain itself to the right strategy (e.g. [DecayingSum]
 * only accepts [HalfLife]).
 *
 * Wall-clock durations travel as `Long` milliseconds rather than
 * `kotlin.time.Duration` to avoid the experimental `Duration` serializer and
 * keep the wire compact.
 */
@Serializable
sealed interface DecayWeightingConfig

@Serializable
@SerialName("HalfLife")
data class HalfLife(val durationMillis: Long) : DecayWeightingConfig {
    fun toDecayWeighting(): DecayWeighting.HalfLife = DecayWeighting.HalfLife(durationMillis.milliseconds)
}

@Serializable
@SerialName("Alpha")
data class Alpha(val alpha: Double) : DecayWeightingConfig {
    fun toDecayWeighting(): DecayWeighting.Alpha = DecayWeighting.Alpha(alpha)
}

@Serializable
@SerialName("DecayingSum")
data class DecayingSum(val weighting: HalfLife) : SeriesStatConfig<DecayingSumResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<DecayingSumResult> =
        DecayingSumStat(weighting.toDecayWeighting(), concurrency)
}

@Serializable
@SerialName("DecayingMean")
data class DecayingMean(val weighting: HalfLife) : SeriesStatConfig<DecayingMeanResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<DecayingMeanResult> =
        DecayingMeanStat(weighting.toDecayWeighting(), concurrency)
}

@Serializable
@SerialName("DecayingVariance")
data class DecayingVariance(val weighting: HalfLife) : SeriesStatConfig<DecayingVarianceResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<DecayingVarianceResult> =
        DecayingVarianceStat(weighting.toDecayWeighting(), concurrency)
}

@Serializable
@SerialName("EwmaMean")
data class EwmaMean(val weighting: Alpha) : SeriesStatConfig<WeightedMeanResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<WeightedMeanResult> =
        EwmaMeanStat(weighting.toDecayWeighting(), concurrency)
}

@Serializable
@SerialName("EwmaVariance")
data class EwmaVariance(val weighting: Alpha) : SeriesStatConfig<WeightedVarianceResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<WeightedVarianceResult> =
        EwmaVarianceStat(weighting.toDecayWeighting(), concurrency)
}

@Serializable
@SerialName("DecayingRate")
data class DecayingRate(val halfLifeMillis: Long) : SeriesStatConfig<DecayingRateResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<DecayingRateResult> =
        DecayingRateStat(halfLifeMillis.milliseconds, concurrency)
}
