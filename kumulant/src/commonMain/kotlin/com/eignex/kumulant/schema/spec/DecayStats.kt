package com.eignex.kumulant.schema.spec

import com.eignex.kumulant.schema.decay.*
import com.eignex.kumulant.stat.decay.DecayingMeanResult
import com.eignex.kumulant.stat.decay.DecayingSumResult
import com.eignex.kumulant.stat.decay.DecayingVarianceResult
import com.eignex.kumulant.stat.forecast.HoltResult
import com.eignex.kumulant.stat.forecast.RecursiveVarianceResult
import com.eignex.kumulant.stat.forecast.SeasonalMode
import com.eignex.kumulant.stat.forecast.SeasonalSmoothingResult
import com.eignex.kumulant.stat.rate.DecayingRateResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Spec for `DecayingSumStat`: time-decayed running sum with [HalfLife] weighting. */
@Serializable
@SerialName("DecayingSum")
data class DecayingSum(
    /** Half-life schedule applied to past contributions. */
    val weighting: HalfLife,
) : SeriesStatSpec<DecayingSumResult>

/** Spec for `DecayingMeanStat`: time-decayed running mean with [HalfLife] weighting. */
@Serializable
@SerialName("DecayingMean")
data class DecayingMean(
    /** Half-life schedule applied to past contributions. */
    val weighting: HalfLife,
) : SeriesStatSpec<DecayingMeanResult>

/** Spec for `DecayingVarianceStat`: time-decayed running variance with [HalfLife] weighting. */
@Serializable
@SerialName("DecayingVariance")
data class DecayingVariance(
    /** Half-life schedule applied to past contributions. */
    val weighting: HalfLife,
) : SeriesStatSpec<DecayingVarianceResult>

/** Spec for `EwmaMeanStat`: exponentially-weighted moving average with per-observation [Alpha]. */
@Serializable
@SerialName("EwmaMean")
data class EwmaMean(
    /** Per-observation smoothing factor. */
    val weighting: Alpha,
) : SeriesStatSpec<WeightedMeanResult>

/** Spec for `EwmaVarianceStat`: exponentially-weighted moving variance with per-observation [Alpha]. */
@Serializable
@SerialName("EwmaVariance")
data class EwmaVariance(
    /** Per-observation smoothing factor. */
    val weighting: Alpha,
) : SeriesStatSpec<WeightedVarianceResult>

/** Spec for `HoltStat`: double exponential smoothing with optional trend damping. */
@Serializable
@SerialName("Holt")
data class Holt(
    /** Per-observation smoothing factor for the level. */
    val alphaWeighting: Alpha,
    /** Per-observation smoothing factor for the trend; defaults to the level's [alphaWeighting]. */
    val betaWeighting: Alpha = alphaWeighting,
    /** Trend damping in `(0, 1]`; `1.0` is plain Holt. */
    val phi: Double = 1.0,
) : SeriesStatSpec<HoltResult>

/** Spec for `SeasonalSmoothingStat`: triple exponential smoothing (Holt-Winters). */
@Serializable
@SerialName("SeasonalSmoothing")
data class SeasonalSmoothing(
    /** Per-observation smoothing factor for the level. */
    val alphaWeighting: Alpha,
    /** Per-observation smoothing factor for the trend. */
    val betaWeighting: Alpha,
    /** Per-observation smoothing factor for the seasonal vector. */
    val gammaWeighting: Alpha,
    /** Length of the seasonal cycle in updates. */
    val period: Int,
    /** Seasonal coupling. */
    val mode: SeasonalMode = SeasonalMode.Additive,
    /** Trend damping in `(0, 1]`. */
    val phi: Double = 1.0,
) : SeriesStatSpec<SeasonalSmoothingResult>

/** Spec for `RecursiveVarianceStat`: `sigma^2_t = omega + alpha * value^2 + beta * sigma^2_{t-1}`. */
@Serializable
@SerialName("RecursiveVariance")
data class RecursiveVariance(
    /** Long-run baseline term. */
    val omega: Double,
    /** Shock coefficient applied to `value^2`. */
    val alpha: Double,
    /** Persistence coefficient applied to the previous variance. */
    val beta: Double,
) : SeriesStatSpec<RecursiveVarianceResult>

/** Spec for `DecayingRateStat`: events-per-second with exponential time decay. */
@Serializable
@SerialName("DecayingRate")
data class DecayingRate(
    /** Half-life of the rate's memory, in milliseconds. */
    val halfLifeMillis: Long,
) : SeriesStatSpec<DecayingRateResult>
