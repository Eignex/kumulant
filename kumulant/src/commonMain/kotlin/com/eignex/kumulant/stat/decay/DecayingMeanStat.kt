package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasObservationCount
import com.eignex.kumulant.core.SeriesStat
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
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
 * **Concurrency:** Inherits [DecayingSumStat]'s lock-free epoch-rotation;
 * exact under every [Concurrency] level. The two sums are updated sequentially
 * without a lock, so a `read()` between them can briefly observe a tiny ratio
 * bias on a contested stream; self-correcting on the next update.
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
            sumW == 0.0 -> 0.0
            else -> Double.NaN
        }
        return DecayingMeanResult(mean, sumW, timestampNanos)
    }

    override fun merge(values: DecayingMeanResult) {
        if (values.totalWeights <= 0.0) return
        sumX.merge(DecayingSumResult(values.mean * values.totalWeights, values.timestampNanos))
        sumW.merge(DecayingSumResult(values.totalWeights, values.timestampNanos))
    }

    override fun reset() {
        sumX.reset()
        sumW.reset()
    }

    override fun create(concurrency: Concurrency?) = DecayingMeanStat(weighting, concurrency ?: this.concurrency)
}
