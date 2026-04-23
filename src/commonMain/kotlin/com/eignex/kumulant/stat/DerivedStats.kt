package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.core.DecayingRateResult
import com.eignex.kumulant.core.DecayingSumResult
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.SumResult
import com.eignex.kumulant.operation.mapResult
import com.eignex.kumulant.operation.withValue
import com.eignex.kumulant.operation.withWeight
import kotlin.math.ln
import kotlin.time.Duration

/** Observation count: each update contributes 1 regardless of supplied value and weight. */
class Count(val mode: StreamMode = defaultStreamMode) :
    SeriesStat<SumResult> by Sum(mode).withWeight(1.0).withValue(1.0)

/** Sum of per-update weights — i.e. the effective sample size. */
class TotalWeights(val mode: StreamMode = defaultStreamMode) :
    SeriesStat<SumResult> by Sum(mode).withValue(1.0)

/**
 * Time-decayed rate with the given [halfLife].
 *
 * Projects [DecayingSum] onto events-per-second via `α = ln 2 / halfLife`, so the
 * rate reflects only the recent window of activity.
 */
class DecayingRate(
    val halfLife: Duration,
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<DecayingRateResult> by decayingRateDelegate(halfLife, mode)

private fun rateScale(halfLife: Duration): Double =
    (ln(2.0) / halfLife.inWholeNanoseconds.toDouble()) * 1e9

private fun decayingRateDelegate(
    halfLife: Duration,
    mode: StreamMode
): SeriesStat<DecayingRateResult> {
    val scale = rateScale(halfLife)
    return DecayingSum(halfLife, mode).mapResult(
        forward = { sum ->
            DecayingRateResult(sum.sum * scale, sum.timestampNanos)
        },
        reverse = { rate ->
            val sum = if (rate.rate <= 0.0) 0.0 else rate.rate / scale
            DecayingSumResult(sum, rate.timestampNanos)
        }
    )
}
