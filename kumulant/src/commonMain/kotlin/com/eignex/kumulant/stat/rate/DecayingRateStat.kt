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

// Per-second rate scale for a half-life. Derived through DecayWeighting.HalfLife so a sub-nanosecond
// half-life raises the documented error rather than yielding an infinite scale.
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
