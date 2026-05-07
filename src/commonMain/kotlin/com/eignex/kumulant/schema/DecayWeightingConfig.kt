package com.eignex.kumulant.schema

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stat.decay.DecayWeighting
import com.eignex.kumulant.stat.decay.DecayingMean
import com.eignex.kumulant.stat.decay.DecayingMeanResult
import com.eignex.kumulant.stat.decay.DecayingSum
import com.eignex.kumulant.stat.decay.DecayingSumResult
import com.eignex.kumulant.stat.decay.DecayingVariance
import com.eignex.kumulant.stat.decay.DecayingVarianceResult
import com.eignex.kumulant.stat.decay.EwmaMean
import com.eignex.kumulant.stat.decay.EwmaVariance
import com.eignex.kumulant.stat.rate.DecayingRate
import com.eignex.kumulant.stat.rate.DecayingRateResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.time.Duration.Companion.milliseconds

/**
 * Wire-friendly counterpart of [DecayWeighting]. The two strategies are split
 * by field type rather than discriminated union so each decay-stat config can
 * statically constrain itself to the right strategy (e.g. [DecayingSumConfig]
 * only accepts [HalfLifeConfig]).
 *
 * Wall-clock durations travel as `Long` milliseconds rather than
 * `kotlin.time.Duration` to avoid the experimental `Duration` serializer and
 * keep the wire compact.
 */
@Serializable
sealed interface DecayWeightingConfig

@Serializable
@SerialName("HalfLife")
data class HalfLifeConfig(val durationMillis: Long) : DecayWeightingConfig {
    fun toDecayWeighting(): DecayWeighting.HalfLife = DecayWeighting.HalfLife(durationMillis.milliseconds)
}

@Serializable
@SerialName("Alpha")
data class AlphaConfig(val alpha: Double) : DecayWeightingConfig {
    fun toDecayWeighting(): DecayWeighting.Alpha = DecayWeighting.Alpha(alpha)
}

@Serializable @SerialName("DecayingSumConfig")
data class DecayingSumConfig(val weighting: HalfLifeConfig) : SeriesStatConfig<DecayingSumResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<DecayingSumResult> =
        DecayingSum(weighting.toDecayWeighting(), concurrency)
}

@Serializable @SerialName("DecayingMeanConfig")
data class DecayingMeanConfig(val weighting: HalfLifeConfig) : SeriesStatConfig<DecayingMeanResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<DecayingMeanResult> =
        DecayingMean(weighting.toDecayWeighting(), concurrency)
}

@Serializable @SerialName("DecayingVarianceConfig")
data class DecayingVarianceConfig(val weighting: HalfLifeConfig) : SeriesStatConfig<DecayingVarianceResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<DecayingVarianceResult> =
        DecayingVariance(weighting.toDecayWeighting(), concurrency)
}

@Serializable @SerialName("EwmaMeanConfig")
data class EwmaMeanConfig(val weighting: AlphaConfig) : SeriesStatConfig<WeightedMeanResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<WeightedMeanResult> =
        EwmaMean(weighting.toDecayWeighting(), concurrency)
}

@Serializable @SerialName("EwmaVarianceConfig")
data class EwmaVarianceConfig(val weighting: AlphaConfig) : SeriesStatConfig<WeightedVarianceResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<WeightedVarianceResult> =
        EwmaVariance(weighting.toDecayWeighting(), concurrency)
}

@Serializable @SerialName("DecayingRateConfig")
data class DecayingRateConfig(val halfLifeMillis: Long) : SeriesStatConfig<DecayingRateResult> {
    override fun materialize(concurrency: Concurrency): SeriesStat<DecayingRateResult> =
        DecayingRate(halfLifeMillis.milliseconds, concurrency)
}
