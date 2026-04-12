package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.StreamDouble
import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.currentTimeNanos
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.core.DecayingMeanResult
import com.eignex.kumulant.core.DecayingRateResult
import com.eignex.kumulant.core.DecayingSumResult
import com.eignex.kumulant.core.DecayingVarianceResult
import com.eignex.kumulant.core.SeriesStat
import kotlin.math.exp
import kotlin.math.ln
import kotlin.time.Duration

/**
 * Exponentially decaying sum: S(t) = Σ vᵢ · e^(−α(t−tᵢ)).
 *
 * The core primitive for all time-decaying statistics. Contributions decay
 * continuously toward zero as real time passes, giving recent observations
 * more influence than older ones. [halfLife] is the time after which any
 * contribution halves in weight; α = ln(2) / halfLife.
 *
 * Use [EwmaMean] / [EwmaVariance] instead when decay should be anchored
 * to observation count rather than wall-clock time.
 */
class DecayingSum(
    val halfLife: Duration,
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<DecayingSumResult> {

    internal val alpha = ln(2.0) / halfLife.inWholeNanoseconds.toDouble()
    private val rotationThresholdNanos = halfLife.inWholeNanoseconds * 50

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
        DecayingSum(halfLife, mode ?: this.mode)
}

/**
 * Exponentially decaying rate estimate: rate(t) = S(t) · α · 1e9 (per second).
 *
 * Interprets the decaying sum of values as an event intensity. In steady state
 * at a constant event rate r (values = 1), the output converges to r events/sec.
 */
class DecayingRate(
    val halfLife: Duration,
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<DecayingRateResult> {

    private val alpha = ln(2.0) / halfLife.inWholeNanoseconds.toDouble()
    private val sum = DecayingSum(halfLife, mode)

    override fun update(value: Double, timestampNanos: Long, weight: Double) =
        sum.update(value, timestampNanos, weight)

    override fun read(timestampNanos: Long): DecayingRateResult {
        val sum = sum.read(timestampNanos).sum
        return DecayingRateResult(sum * alpha * 1e9, timestampNanos)
    }

    override fun merge(values: DecayingRateResult) {
        if (values.rate <= 0.0) return
        val addedSum = values.rate / (alpha * 1e9)
        sum.merge(DecayingSumResult(addedSum, values.timestampNanos))
    }

    override fun reset() = sum.reset()

    override fun create(mode: StreamMode?) =
        DecayingRate(halfLife, mode ?: this.mode)
}

/**
 * Exponentially decaying weighted mean: mean(t) = Σ(vᵢ·wᵢ·decay) / Σ(wᵢ·decay).
 *
 * Uses two [DecayingSum]s internally — one for weighted values, one for weights.
 * Older observations fade out continuously; there is no sharp cut-off.
 */
class DecayingMean(
    val halfLife: Duration,
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<DecayingMeanResult> {

    private val sumX = DecayingSum(halfLife, mode)
    private val sumW = DecayingSum(halfLife, mode)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        sumX.update(value, timestampNanos, weight)
        sumW.update(1.0, timestampNanos, weight)
    }

    override fun read(timestampNanos: Long): DecayingMeanResult {
        val sumX = sumX.read(timestampNanos).sum
        val sumW = sumW.read(timestampNanos).sum
        val mean = if (sumW > 0.0) sumX / sumW else 0.0
        return DecayingMeanResult(mean, sumW, timestampNanos)
    }

    override fun merge(values: DecayingMeanResult) {
        if (values.decayingCount <= 0.0) return
        sumX.merge(DecayingSumResult(values.mean * values.decayingCount, values.timestampNanos))
        sumW.merge(DecayingSumResult(values.decayingCount, values.timestampNanos))
    }

    override fun reset() {
        sumX.reset()
        sumW.reset()
    }

    override fun create(mode: StreamMode?) =
        DecayingMean(halfLife, mode ?: this.mode)
}

/**
 * Exponentially decaying weighted variance.
 *
 * Uses three [DecayingSum]s — for x², x, and weights — to compute
 * variance = E[x²] − E[x]² over the exponentially windowed distribution.
 */
class DecayingVariance(
    val halfLife: Duration,
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<DecayingVarianceResult> {

    private val sumX2 = DecayingSum(halfLife, mode)
    private val sumX = DecayingSum(halfLife, mode)
    private val sumW = DecayingSum(halfLife, mode)

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
        if (values.decayingCount <= 0.0) return
        val sumW = values.decayingCount
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
        DecayingVariance(halfLife, mode ?: this.mode)
}
