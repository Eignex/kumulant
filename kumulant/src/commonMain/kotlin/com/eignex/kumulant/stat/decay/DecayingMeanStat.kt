package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasObservationCount
import com.eignex.kumulant.core.SeriesStat
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.exp
import kotlin.time.Duration

/** Snapshot of an exponentially time-decayed weighted mean at [timestampNanos]. */
@Serializable
@SerialName("DecayingMeanResult")
data class DecayingMeanResult(
    /** Time-decayed weighted running mean. */
    val mean: Double,
    /** Effective weight of observations still contributing (decays with time). */
    override val totalWeights: Double,
    /** Wall-clock timestamp (nanoseconds) at which the snapshot was taken. */
    val timestampNanos: Long,
) : HasObservationCount

/**
 * Exponentially decaying weighted mean: `Sum(v_i*w_i*decay) / Sum(w_i*decay)`.
 *
 * Composes two [DecayingSumStat]s; one for weighted values, one for weights; so
 * that the decay factor cancels in the ratio and the mean reflects only the
 * *relative* weighting of recent vs. older observations.
 *
 * **Use cases:** recency-biased central tendency (rolling average of latencies,
 * recent click-through rate, etc.). Reach for this over [com.eignex.kumulant.stat.summary.MeanStat] when older
 * observations should fade rather than persist.
 *
 * **Memory:** O(1); two `DecayingSumStat` instances.
 *
 * **Update:** O(1) per observation (two `DecayingSumStat.update()` calls).
 *
 * **Concurrency:** Each underlying [DecayingSumStat] locks its state under every
 * concurrent level. The numerator and denominator are updated and read separately,
 * so concurrent readers can observe a partial pair. After writers finish, both sums
 * include every accepted contribution. [Concurrency.None] runs without synchronisation.
 */
class DecayingMeanStat(
    /** Time-decay schedule applied to past contributions. */
    val weighting: DecayWeighting.HalfLife,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<DecayingMeanResult> {

    constructor(halfLife: Duration, concurrency: Concurrency = Concurrency.None) :
        this(DecayWeighting.HalfLife(halfLife), concurrency)

    /** Wall-clock half-life of past contributions. */
    val halfLife: Duration get() = weighting.halfLife

    private val alpha = weighting.alpha
    private val sumX = DecayingSumStat(weighting, concurrency)
    private val sumW = DecayingSumStat(weighting, concurrency)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        sumX.update(value, timestampNanos, weight)
        sumW.update(1.0, timestampNanos, weight)
    }

    override fun read(timestampNanos: Long): DecayingMeanResult {
        val sumX = sumX.read(timestampNanos).sum
        val sumW = sumW.read(timestampNanos).sum
        val mean = when {
            sumW > 0.0 -> sumX / sumW

            sumW < 0.0 -> Double.NaN

            // A NaN denominator is neither greater nor less than zero, so without this branch it
            // falls through to the underflow branch and reports a mean of exactly 0.0,
            // indistinguishable from a genuine zero. The underflow branch is for a denominator that
            // decayed to 0.0, which is a different state and the only one with a recoverable answer.
            sumW.isNaN() -> Double.NaN

            else -> undecayedRatio()
        }
        return DecayingMeanResult(mean, sumW, timestampNanos)
    }

    // Undecayed sums retain the mean after both decayed sums underflow; totalWeights still reports zero.
    private fun undecayedRatio(): Double {
        val rawX = sumX.undecayed()
        val rawW = sumW.undecayed()
        if (rawW.sum <= 0.0) return 0.0
        // The two sums are separate instances, so they take their landmarks microseconds apart and
        // rotate independently. Discounting both to a common time leaves only the landmark
        // *difference* in the exponent, which is small - unlike the elapsed time, which is what
        // underflowed in the first place.
        val offset = exp(-alpha * (rawW.landmarkNanos - rawX.landmarkNanos).toDouble())
        val ratio = (rawX.sum / rawW.sum) * offset
        return if (ratio.isFinite()) ratio else 0.0
    }

    override fun merge(values: DecayingMeanResult, timestampNanos: Long) {
        if (values.totalWeights <= 0.0) return
        val halfLifeNanos = weighting.halfLife.inWholeNanoseconds
        sumX.merge(DecayingSumResult(values.mean * values.totalWeights, values.timestampNanos, halfLifeNanos), timestampNanos)
        sumW.merge(DecayingSumResult(values.totalWeights, values.timestampNanos, halfLifeNanos), timestampNanos)
    }

    override fun reset() {
        sumX.reset()
        sumW.reset()
    }

    override fun create(concurrency: Concurrency?) = DecayingMeanStat(weighting, concurrency ?: this.concurrency)
}
