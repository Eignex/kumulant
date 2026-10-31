package com.eignex.kumulant.schema.spec

import com.eignex.kumulant.stat.StatDefaults
import com.eignex.kumulant.stat.decay.DecayingMeanResult
import com.eignex.kumulant.stat.decay.DecayingSumResult
import com.eignex.kumulant.stat.decay.DecayingVarianceResult
import com.eignex.kumulant.stat.forecast.ExponentialSmoothingResult
import com.eignex.kumulant.stat.forecast.RecursiveVarianceResult
import com.eignex.kumulant.stat.forecast.SeasonalMode
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
    /**
     * Decay rate per unit of observation weight: each update moves the mean by `1 - exp(-alpha * weight)`.
     * Any finite value `>= 0`; `0` keeps the mean where it is.
     */
    val alpha: Double,
) : SeriesStatSpec<WeightedMeanResult>

/** Spec for `EwmaVarianceStat`: exponentially-weighted moving variance with per-observation alpha. */
@Serializable
@SerialName("EwmaVariance")
data class EwmaVariance(
    /**
     * Decay rate per unit of observation weight: each update moves the estimates by `1 - exp(-alpha * weight)`.
     * Any finite value `>= 0`; `0` keeps them where they are.
     */
    val alpha: Double,
) : SeriesStatSpec<WeightedVarianceResult>

/**
 * Spec for `ExponentialSmoothingStat`: exponential smoothing with a damped trend and optional
 * seasonality. [period] `1` is Holt's method; a longer period adds a Holt-Winters seasonal cycle.
 */
@Serializable
@SerialName("ExponentialSmoothing")
data class ExponentialSmoothing(
    /** Level decay rate per unit of weight; the level step is `1 - exp(-alpha * weight)`. Finite and `>= 0`. */
    val alpha: Double,
    /** Trend decay rate per unit of weight, stepping by `1 - exp(-beta * weight)`; defaults to [alpha]. */
    val beta: Double = alpha,
    /** Trend damping in `(0, 1]`; `1.0` is an undamped trend. */
    val phi: Double = StatDefaults.SMOOTHING_PHI,
    /** Length of the seasonal cycle in updates; `1` disables seasonality. */
    val period: Int = StatDefaults.SMOOTHING_PERIOD,
    /** Seasonal decay rate per unit of weight, stepping by `1 - exp(-gamma * weight)`; defaults to [alpha]. */
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
