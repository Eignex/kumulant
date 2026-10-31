package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasRate
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.stream.NANOS_PER_SECOND
import com.eignex.kumulant.stream.currentTimeNanos
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.serializedLock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.exp
import kotlin.math.ln
import kotlin.time.Duration

/**
 * Snapshot of an exponentially time-decayed sum at [timestampNanos].
 *
 * [rate] reads the sum as a decayed throughput, `sum * ln(2) / halfLife` per second: for
 * unit values that is events per second, weighted toward the last few half-lives.
 */
@Serializable
@SerialName("DecayingSumResult")
data class DecayingSumResult(
    /** Time-decayed sum at [timestampNanos]. */
    val sum: Double,
    /** Wall-clock timestamp (nanoseconds) at which the snapshot was taken. */
    val timestampNanos: Long,
    /** Half-life of past contributions, in nanoseconds. */
    val halfLifeNanos: Long,
) : HasRate {
    override val rate: Double get() = sum * LN_2 / halfLifeNanos * NANOS_PER_SECOND
}

private val LN_2 = ln(2.0)

/**
 * Exponentially decaying sum driven by wall-clock elapsed time.
 *
 * `S(t) = Sum v_i * w_i * exp(-alpha*(t - t_i))` with `alpha = ln(2)/halfLife`.
 *
 * Internally uses landmark rotation to keep the stored accumulator in a
 * bounded numerical range even after many half-lives of activity.
 *
 * **Use cases:** time-windowed event totals (requests in the last 30s),
 * recency-weighted aggregation for monitoring dashboards, and recent throughput through
 * [DecayingSumResult.rate] (request rate over the last 30 s, recent error rate). The core
 * time-decay primitive; pair with another [DecayingSumStat] for ratios via [DecayingMeanStat].
 *
 * **Memory:** O(1); a landmark, a sum, and a lock.
 *
 * **Update:** O(1) per observation, including landmark rotation.
 *
 * **Concurrency:** Body locked under any concurrent level. Updates, landmark rotation,
 * reads, merges, and resets share one lock so a contribution cannot land in retired
 * storage. [Concurrency.None] runs without synchronisation.
 */
class DecayingSumStat(
    /** Time-decay schedule applied to past contributions. */
    val weighting: DecayWeighting.HalfLife,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<DecayingSumResult> {

    constructor(halfLife: Duration, concurrency: Concurrency = Concurrency.None) :
        this(DecayWeighting.HalfLife(halfLife), concurrency)

    /** Wall-clock half-life of past contributions. */
    val halfLife: Duration get() = weighting.halfLife
    private val halfLifeNanos = weighting.halfLife.inWholeNanoseconds
    private val alpha = weighting.alpha

    // A wrapped negative threshold would rotate on ordinary late arrivals.
    // Huge half-lives need no rotation within the representable timestamp span.
    private val rotationThresholdNanos = weighting.halfLife.inWholeNanoseconds.let { halfLifeNanos ->
        if (halfLifeNanos > Long.MAX_VALUE / ROTATION_HALF_LIVES) {
            Long.MAX_VALUE
        } else {
            halfLifeNanos * ROTATION_HALF_LIVES
        }
    }

    private val lock = concurrency.serializedLock()
    private var landmarkNanos = currentTimeNanos()
    private var sum = 0.0

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        lock.guarded { addAt(value * weight, timestampNanos) }
    }

    private fun addAt(value: Double, timestampNanos: Long) {
        // An empty accumulator can adopt a replay clock without losing history.
        if (sum == 0.0 && timestampNanos < landmarkNanos) landmarkNanos = timestampNanos
        if (timestampNanos - landmarkNanos > rotationThresholdNanos) {
            sum = decayedSum(timestampNanos)
            landmarkNanos = timestampNanos
        }
        sum += value * exp(alpha * (timestampNanos - landmarkNanos).toDouble())
    }

    override fun read(timestampNanos: Long): DecayingSumResult = lock.guarded {
        DecayingSumResult(decayedSum(timestampNanos), timestampNanos, halfLifeNanos)
    }

    private fun decayedSum(timestampNanos: Long): Double {
        // A zero accumulator must not become NaN when a backwards read overflows the factor.
        if (sum == 0.0) return 0.0
        return sum * exp(-alpha * (timestampNanos - landmarkNanos).toDouble())
    }

    internal class Undecayed(val landmarkNanos: Long, val sum: Double)

    internal fun undecayed(): Undecayed = lock.guarded { Undecayed(landmarkNanos, sum) }

    override fun merge(values: DecayingSumResult, timestampNanos: Long) {
        if (values.sum == 0.0) return
        lock.guarded { addAt(values.sum, values.timestampNanos) }
    }

    override fun reset() = lock.guarded {
        landmarkNanos = currentTimeNanos()
        sum = 0.0
    }

    override fun create(concurrency: Concurrency?) = DecayingSumStat(weighting, concurrency ?: this.concurrency)

    private companion object {
        const val ROTATION_HALF_LIVES = 50L
    }
}
