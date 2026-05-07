package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.time.Duration

/** Snapshot of an exponentially time-decayed weighted mean at [timestampNanos]. */
@Serializable
@SerialName("DecayingMeanResult")
data class DecayingMeanResult(
    val mean: Double,
    /** Effective weight of observations still contributing (decays with time). */
    val totalWeights: Double,
    val timestampNanos: Long,
) : Result

/**
 * Exponentially decaying weighted mean: `Σ(vᵢ·wᵢ·decay) / Σ(wᵢ·decay)`.
 *
 * Composes two [DecayingSumStat]s — one for weighted values, one for weights — so that the
 * decay factor cancels in the ratio and the mean reflects only the *relative* weighting
 * of recent vs. older observations.
 */
class DecayingMeanStat(
    val weighting: DecayWeighting.HalfLife,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<DecayingMeanResult> {

    constructor(halfLife: Duration, concurrency: Concurrency = Concurrency.None) :
        this(DecayWeighting.HalfLife(halfLife), concurrency)

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

    override fun create(concurrency: Concurrency?) =
        DecayingMeanStat(weighting, concurrency ?: this.concurrency)
}
