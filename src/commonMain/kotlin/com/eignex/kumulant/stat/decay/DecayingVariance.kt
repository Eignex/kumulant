package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.currentTimeNanos
import com.eignex.kumulant.stream.defaultStreamMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.exp
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
 * Holds a Welford-style accumulator `(W, mean, M2)` whose effective weight and
 * second-central-moment are decayed in lockstep with elapsed wall-clock time.
 * Each update advances the landmark to the event timestamp, decays `W` and `M2`
 * by `exp(−α·Δt)`, then applies the standard increment
 * `M2 += w·δ·(value − meanNew)`. This avoids the catastrophic cancellation of
 * the `E[X²] − E[X]²` form when `stdDev ≪ |mean|`.
 */
class DecayingVariance(
    val weighting: DecayWeighting.HalfLife,
    override val mode: StreamMode = defaultStreamMode,
) : SeriesStat<DecayingVarianceResult> {

    constructor(halfLife: Duration, mode: StreamMode = defaultStreamMode) :
        this(DecayWeighting.HalfLife(halfLife), mode)

    val halfLife: Duration get() = weighting.halfLife
    private val alpha = weighting.alpha

    private val landmarkNanos = mode.newLong(currentTimeNanos())
    private val totalWeights = mode.newDouble(0.0)
    private val mean = mode.newDouble(0.0)
    private val m2 = mode.newDouble(0.0)

    private fun advanceTo(t: Long) {
        val priorLandmark = landmarkNanos.load()
        if (t == priorLandmark) return
        val priorW = totalWeights.load()
        if (priorW == 0.0) {
            landmarkNanos.store(t)
            return
        }
        val decay = exp(-alpha * (t - priorLandmark).toDouble())
        totalWeights.store(priorW * decay)
        m2.store(m2.load() * decay)
        landmarkNanos.store(t)
    }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0) return
        advanceTo(timestampNanos)
        val priorW = totalWeights.load()
        val nextW = priorW + weight
        val priorMean = mean.load()
        val delta = value - priorMean
        val newMean = priorMean + delta * weight / nextW
        totalWeights.store(nextW)
        mean.store(newMean)
        m2.add(weight * delta * (value - newMean))
    }

    override fun read(timestampNanos: Long): DecayingVarianceResult {
        val landmark = landmarkNanos.load()
        val priorW = totalWeights.load()
        val priorM2 = m2.load()
        val w: Double
        val decayedM2: Double
        if (priorW == 0.0 || timestampNanos == landmark) {
            w = priorW
            decayedM2 = priorM2
        } else {
            val decay = exp(-alpha * (timestampNanos - landmark).toDouble())
            w = priorW * decay
            decayedM2 = priorM2 * decay
        }
        val variance = if (w > 0.0) decayedM2 / w else 0.0
        return DecayingVarianceResult(mean.load(), variance, w, timestampNanos)
    }

    override fun merge(values: DecayingVarianceResult) {
        if (values.totalWeights <= 0.0) return
        val target = maxOf(landmarkNanos.load(), values.timestampNanos)
        advanceTo(target)
        val remoteDecay = exp(-alpha * (target - values.timestampNanos).toDouble())
        val remoteW = values.totalWeights * remoteDecay
        val remoteM2 = values.variance * remoteW
        val w1 = totalWeights.load()
        val nextW = w1 + remoteW
        if (nextW == 0.0) return
        val priorMean = mean.load()
        val delta = values.mean - priorMean
        mean.store(priorMean + delta * remoteW / nextW)
        totalWeights.store(nextW)
        m2.add(remoteM2 + delta * delta * w1 * remoteW / nextW)
    }

    override fun reset() {
        landmarkNanos.store(currentTimeNanos())
        totalWeights.store(0.0)
        mean.store(0.0)
        m2.store(0.0)
    }

    override fun create(mode: StreamMode?) =
        DecayingVariance(weighting, mode ?: this.mode)
}
