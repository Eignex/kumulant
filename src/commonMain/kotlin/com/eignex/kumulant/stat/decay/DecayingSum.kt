package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.StreamDouble
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.currentTimeNanos
import com.eignex.kumulant.stream.defaultStreamMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.exp
import kotlin.time.Duration

/** Snapshot of an exponentially time-decayed sum at [timestampNanos]. */
@Serializable
@SerialName("DecayingSum")
data class DecayingSumResult(
    val sum: Double,
    val timestampNanos: Long,
) : Result

// Time-decayed family (HalfLife weighting).
// S(t) = Σ vᵢ · wᵢ · exp(−α·(t − tᵢ)) with α = ln(2)/halfLife. Decay advances with
// wall-clock time regardless of event frequency. See [DecayWeighting.HalfLife].

/**
 * Exponentially decaying sum driven by wall-clock elapsed time.
 *
 * The core time-decay primitive. Internally uses landmark-rotation to keep the stored
 * accumulator in a bounded numerical range even after many half-lives of activity.
 */
class DecayingSum(
    val weighting: DecayWeighting.HalfLife,
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<DecayingSumResult> {

    constructor(halfLife: Duration, mode: StreamMode = defaultStreamMode) :
        this(DecayWeighting.HalfLife(halfLife), mode)

    val halfLife: Duration get() = weighting.halfLife
    private val alpha = weighting.alpha
    private val rotationThresholdNanos = weighting.halfLife.inWholeNanoseconds * ROTATION_HALF_LIVES

    private class Epoch(val landmarkNanos: Long, val accumulator: StreamDouble)

    private val epochRef = mode.newReference(
        Epoch(currentTimeNanos(), mode.newDouble(0.0))
    )

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        while (true) {
            val epoch = epochRef.load()
            if (timestampNanos - epoch.landmarkNanos > rotationThresholdNanos) {
                tryRotateEpoch(epoch, timestampNanos)
                continue
            }
            val dt = timestampNanos - epoch.landmarkNanos
            epoch.accumulator.add(value * weight * exp(alpha * dt))
            return
        }
    }

    private fun tryRotateEpoch(old: Epoch, now: Long) {
        val dt = now - old.landmarkNanos
        val carried = old.accumulator.load() * exp(-alpha * dt)
        epochRef.compareAndSet(old, Epoch(now, mode.newDouble(carried)))
    }

    override fun read(timestampNanos: Long): DecayingSumResult {
        val epoch = epochRef.load()
        val dt = (timestampNanos - epoch.landmarkNanos).toDouble()
        val sum = epoch.accumulator.load() * exp(-alpha * dt)
        return DecayingSumResult(sum, timestampNanos)
    }

    override fun merge(values: DecayingSumResult) {
        if (values.sum == 0.0) return
        while (true) {
            val epoch = epochRef.load()
            val now = values.timestampNanos
            if (now - epoch.landmarkNanos <= rotationThresholdNanos) {
                val dt = (now - epoch.landmarkNanos).toDouble()
                epoch.accumulator.add(values.sum * exp(alpha * dt))
                break
            }
            tryRotateEpoch(epoch, now)
        }
    }

    override fun reset() {
        val current = epochRef.load()
        epochRef.compareAndSet(current, Epoch(currentTimeNanos(), mode.newDouble(0.0)))
    }

    override fun create(mode: StreamMode?) =
        DecayingSum(weighting, mode ?: this.mode)

    private companion object {
        const val ROTATION_HALF_LIVES = 50L
    }
}
