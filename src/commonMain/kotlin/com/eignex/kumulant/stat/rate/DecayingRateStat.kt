package com.eignex.kumulant.stat.rate

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasRate
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.operation.mapResult
import com.eignex.kumulant.stat.decay.DecayingSumStat
import com.eignex.kumulant.stat.decay.DecayingSumResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.ln
import kotlin.time.Duration

/** Exponentially time-decayed rate snapshot. */
@Serializable
@SerialName("DecayingRateStat")
data class DecayingRateResult(
    override val rate: Double,
    val timestampNanos: Long,
) : Result, HasRate

/**
 * Time-decayed rate with the given [halfLife].
 *
 * Projects [DecayingSumStat] onto events-per-second via `α = ln 2 / halfLife`, so the
 * rate reflects only the recent window of activity.
 */
class DecayingRateStat(
    val halfLife: Duration,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<DecayingRateResult> by decayingRateDelegate(halfLife, concurrency)

private fun rateScale(halfLife: Duration): Double =
    (ln(2.0) / halfLife.inWholeNanoseconds.toDouble()) * 1e9

private fun decayingRateDelegate(
    halfLife: Duration,
    concurrency: Concurrency
): SeriesStat<DecayingRateResult> {
    val scale = rateScale(halfLife)
    return DecayingSumStat(halfLife, concurrency).mapResult(
        forward = { sum ->
            DecayingRateResult(sum.sum * scale, sum.timestampNanos)
        },
        reverse = { rate ->
            val sum = if (rate.rate <= 0.0) 0.0 else rate.rate / scale
            DecayingSumResult(sum, rate.timestampNanos)
        }
    )
}
