package com.eignex.kumulant.schema.spec

import com.eignex.kumulant.stat.StatDefaults
import com.eignex.kumulant.stat.decay.DecayingMeanResult
import com.eignex.kumulant.stat.decay.DecayingSumResult
import com.eignex.kumulant.stat.decay.DecayingVarianceResult
import com.eignex.kumulant.stat.forecast.ExponentialSmoothingResult
import com.eignex.kumulant.stat.forecast.RecursiveVarianceResult
import com.eignex.kumulant.stat.forecast.SeasonalMode
import com.eignex.kumulant.stat.rate.DecayingRateResult
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Spec for `DecayingSumStat`: time-decayed running sum with half-life weighting. */
@Serializable
@SerialName("DecayingSum")
data class DecayingSum(
    /** Half-life of past contributions, in milliseconds. */
    val halfLifeMillis: Long,
) : SeriesStatSpec<DecayingSumResult>

/** Spec for `DecayingMeanStat`: time-decayed running mean with half-life weighting. */
@Serializable
@SerialName("DecayingMean")
data class DecayingMean(
    /** Half-life of past contributions, in milliseconds. */
    val halfLifeMillis: Long,
) : SeriesStatSpec<DecayingMeanResult>

/** Spec for `DecayingVarianceStat`: time-decayed running variance with half-life weighting. */
@Serializable
@SerialName("DecayingVariance")
data class DecayingVariance(
    /** Half-life of past contributions, in milliseconds. */
    val halfLifeMillis: Long,
) : SeriesStatSpec<DecayingVarianceResult>

/** Spec for `EwmaMeanStat`: exponentially-weighted moving average with per-observation alpha. */
@Serializable
@SerialName("EwmaMean")
data class EwmaMean(
    /** Per-observation smoothing factor in `(0, 1]`. */
    val alpha: Double,
) : SeriesStatSpec<WeightedMeanResult>

/** Spec for `EwmaVarianceStat`: exponentially-weighted moving variance with per-observation alpha. */
@Serializable
@SerialName("EwmaVariance")
data class EwmaVariance(
    /** Per-observation smoothing factor in `(0, 1]`. */
    val alpha: Double,
) : SeriesStatSpec<WeightedVarianceResult>

/**
 * Spec for `ExponentialSmoothingStat`: exponential smoothing with a damped trend and optional
 * seasonality. [period] `1` is Holt's method; a longer period adds a Holt-Winters seasonal cycle.
 */
@Serializable
@SerialName("ExponentialSmoothing")
data class ExponentialSmoothing(
    /** Per-observation smoothing factor for the level. */
    val alpha: Double,
    /** Per-observation smoothing factor for the trend; defaults to the level's [alpha]. */
    val beta: Double = alpha,
    /** Trend damping in `(0, 1]`; `1.0` is an undamped trend. */
    val phi: Double = StatDefaults.SMOOTHING_PHI,
    /** Length of the seasonal cycle in updates; `1` disables seasonality. */
    val period: Int = StatDefaults.SMOOTHING_PERIOD,
    /** Per-observation smoothing factor for the seasonal vector; defaults to the level's [alpha]. */
    val gamma: Double = alpha,
    /** Seasonal coupling. */
    val mode: SeasonalMode = StatDefaults.seasonalMode,
) : SeriesStatSpec<ExponentialSmoothingResult>

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
