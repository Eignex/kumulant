package com.eignex.kumulant.stat.rate

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasRate
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.operation.mapResult
import com.eignex.kumulant.stat.decay.DecayWeighting
import com.eignex.kumulant.stat.decay.DecayingSumResult
import com.eignex.kumulant.stat.decay.DecayingSumStat
import com.eignex.kumulant.stream.NANOS_PER_SECOND
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.time.Duration

/** Exponentially time-decayed rate snapshot. */
@Serializable
@SerialName("DecayingRateResult")
data class DecayingRateResult(
    override val rate: Double,
    /** Wall-clock timestamp (nanoseconds) of the snapshot, for downstream extrapolation. */
    val timestampNanos: Long,
) : Result,
    HasRate

/**
 * Time-decayed rate with the given [halfLife].
 *
 * Projects [DecayingSumStat] onto events-per-second via `alpha = ln 2 / halfLife`,
 * so the rate reflects only the recent window of activity.
 *
 * **Use cases:** recent throughput / events-per-second (request rate over the
 * last 30 s, recent error rate). Reach for this over [RateStat] when older
 * activity should fade.
 *
 * **Memory:** O(1); one [DecayingSumStat] plus a scalar projection.
 *
 * **Update:** O(1) per observation (one [DecayingSumStat.update] call).
 *
 * **Concurrency:** Inherits [DecayingSumStat]'s concurrency model; lock-free
 * and exact under every [Concurrency] level.
 */
class DecayingRateStat(val halfLife: Duration, override val concurrency: Concurrency = Concurrency.None) :
    SeriesStat<DecayingRateResult> by decayingRateDelegate(halfLife, concurrency)

/**
 * Per-second rate scale for a half-life, via [DecayWeighting.HalfLife].
 *
 * This recomputed `ln(2) / halfLife` itself rather than reading the one place that already derives it,
 * and the copy had no validation: `inWholeNanoseconds` truncates, so a sub-nanosecond half-life gave an
 * infinite scale instead of the documented error. It only ever looked correct because the
 * `DecayingSumStat` constructed on the next line happens to throw first - an ordering the compiler does
 * not enforce and nothing recorded.
 */
private fun rateScale(halfLife: Duration): Double = DecayWeighting.HalfLife(halfLife).alpha * NANOS_PER_SECOND

private fun decayingRateDelegate(halfLife: Duration, concurrency: Concurrency): SeriesStat<DecayingRateResult> {
    val scale = rateScale(halfLife)
    return DecayingSumStat(halfLife, concurrency).mapResult(
        forward = { sum ->
            DecayingRateResult(sum.sum * scale, sum.timestampNanos)
        },
        reverse = { rate ->
            DecayingSumResult(rate.rate / scale, rate.timestampNanos)
        },
    )
}
