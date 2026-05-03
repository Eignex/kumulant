package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.StreamRef
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

    private data class State(
        val timestampNanos: Long,
        val totalWeights: Double,
        val mean: Double,
        val m2: Double,
    )

    private val stateRef: StreamRef<State> =
        mode.newReference(State(currentTimeNanos(), 0.0, 0.0, 0.0))

    private fun advance(s: State, t: Long): State {
        if (s.totalWeights == 0.0) return State(t, 0.0, s.mean, 0.0)
        val dt = (t - s.timestampNanos).toDouble()
        val decay = exp(-alpha * dt)
        return State(t, s.totalWeights * decay, s.mean, s.m2 * decay)
    }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0) return
        while (true) {
            val s = stateRef.load()
            val advanced = advance(s, timestampNanos)
            val delta = value - advanced.mean
            val newW = advanced.totalWeights + weight
            val newMean = advanced.mean + delta * weight / newW
            val newM2 = advanced.m2 + weight * delta * (value - newMean)
            val next = State(timestampNanos, newW, newMean, newM2)
            if (stateRef.compareAndSet(s, next)) return
        }
    }

    override fun read(timestampNanos: Long): DecayingVarianceResult {
        val s = stateRef.load()
        val advanced = advance(s, timestampNanos)
        val variance =
            if (advanced.totalWeights > 0.0) advanced.m2 / advanced.totalWeights else 0.0
        return DecayingVarianceResult(advanced.mean, variance, advanced.totalWeights, timestampNanos)
    }

    override fun merge(values: DecayingVarianceResult) {
        if (values.totalWeights <= 0.0) return
        while (true) {
            val s = stateRef.load()
            val target = maxOf(s.timestampNanos, values.timestampNanos)
            val local = advance(s, target)
            val remoteDecay = exp(-alpha * (target - values.timestampNanos).toDouble())
            val remoteW = values.totalWeights * remoteDecay
            val remoteM2 = values.variance * remoteW
            val newW = local.totalWeights + remoteW
            if (newW == 0.0) return
            val delta = values.mean - local.mean
            val newMean = local.mean + delta * remoteW / newW
            val newM2 =
                local.m2 + remoteM2 + delta * delta * local.totalWeights * remoteW / newW
            val next = State(target, newW, newMean, newM2)
            if (stateRef.compareAndSet(s, next)) return
        }
    }

    override fun reset() {
        stateRef.store(State(currentTimeNanos(), 0.0, 0.0, 0.0))
    }

    override fun create(mode: StreamMode?) =
        DecayingVariance(weighting, mode ?: this.mode)
}
