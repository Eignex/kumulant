package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.defaultStreamMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.time.Duration

/** Snapshot of an exponentially time-decayed weighted mean at [timestampNanos]. */
@Serializable
@SerialName("DecayingMean")
data class DecayingMeanResult(
    val mean: Double,
    /** Effective weight of observations still contributing (decays with time). */
    val totalWeights: Double,
    val timestampNanos: Long,
) : Result

/**
 * Exponentially decaying weighted mean: `Σ(vᵢ·wᵢ·decay) / Σ(wᵢ·decay)`.
 *
 * Composes two [DecayingSum]s — one for weighted values, one for weights — so that the
 * decay factor cancels in the ratio and the mean reflects only the *relative* weighting
 * of recent vs. older observations.
 */
class DecayingMean(
    val weighting: DecayWeighting.HalfLife,
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<DecayingMeanResult> {

    constructor(halfLife: Duration, mode: StreamMode = defaultStreamMode) :
        this(DecayWeighting.HalfLife(halfLife), mode)

    val halfLife: Duration get() = weighting.halfLife

    private val sumX = DecayingSum(weighting, mode)
    private val sumW = DecayingSum(weighting, mode)

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

    override fun create(mode: StreamMode?) =
        DecayingMean(weighting, mode ?: this.mode)
}
