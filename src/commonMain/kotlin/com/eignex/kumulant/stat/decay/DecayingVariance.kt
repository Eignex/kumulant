package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.defaultStreamMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.sqrt
import kotlin.time.Duration

/** Snapshot of an exponentially time-decayed weighted variance at [timestampNanos]. */
@Serializable
@SerialName("DecayingVariance")
data class DecayingVarianceResult(
    val mean: Double,
    val variance: Double,
    /** Effective weight of observations still contributing (decays with time). */
    val totalWeights: Double,
    val timestampNanos: Long,
) : Result {
    /** Square root of [variance]. */
    val stdDev: Double get() = sqrt(variance)
}

/**
 * Exponentially decaying weighted variance over the recent time window.
 *
 * Composes three [DecayingSum]s — for x², x, and weights — to compute
 * variance = E[x²] − E[x]² over the exponentially windowed distribution.
 */
class DecayingVariance(
    val weighting: DecayWeighting.HalfLife,
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<DecayingVarianceResult> {

    constructor(halfLife: Duration, mode: StreamMode = defaultStreamMode) :
        this(DecayWeighting.HalfLife(halfLife), mode)

    val halfLife: Duration get() = weighting.halfLife

    private val sumX2 = DecayingSum(weighting, mode)
    private val sumX = DecayingSum(weighting, mode)
    private val sumW = DecayingSum(weighting, mode)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        sumX2.update(value * value, timestampNanos, weight)
        sumX.update(value, timestampNanos, weight)
        sumW.update(1.0, timestampNanos, weight)
    }

    override fun read(timestampNanos: Long): DecayingVarianceResult {
        val sumX2 = sumX2.read(timestampNanos).sum
        val sumX = sumX.read(timestampNanos).sum
        val sumW = sumW.read(timestampNanos).sum
        val mean = if (sumW > 0.0) sumX / sumW else 0.0
        val variance = if (sumW > 0.0) (sumX2 / sumW - mean * mean).coerceAtLeast(0.0) else 0.0
        return DecayingVarianceResult(mean, variance, sumW, timestampNanos)
    }

    override fun merge(values: DecayingVarianceResult) {
        if (values.totalWeights <= 0.0) return
        val sumW = values.totalWeights
        val sumX = values.mean * sumW
        val sumX2 = (values.variance + values.mean * values.mean) * sumW
        this.sumX2.merge(DecayingSumResult(sumX2, values.timestampNanos))
        this.sumX.merge(DecayingSumResult(sumX, values.timestampNanos))
        this.sumW.merge(DecayingSumResult(sumW, values.timestampNanos))
    }

    override fun reset() {
        sumX2.reset()
        sumX.reset()
        sumW.reset()
    }

    override fun create(mode: StreamMode?) =
        DecayingVariance(weighting, mode ?: this.mode)
}
