package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.StreamDouble
import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.currentTimeNanos
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.core.DecayingMeanResult
import com.eignex.kumulant.core.DecayingRateResult
import com.eignex.kumulant.core.DecayingSumResult
import com.eignex.kumulant.core.DecayingVarianceResult
import com.eignex.kumulant.core.HasMean
import com.eignex.kumulant.core.HasRate
import com.eignex.kumulant.core.HasVariance
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
    override val name: String? = null,
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
        return DecayingSumResult(sum, timestampNanos, name)
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

    override fun create(mode: StreamMode?, name: String?) =
        DecayingSum(halfLife, mode ?: this.mode, name ?: this.name)
}

/**
 * Exponentially decaying rate estimate: rate(t) = S(t) · α · 1e9 (per second).
 *
 * Interprets the decaying sum of values as an event intensity. In steady state
 * at a constant event rate r (values = 1), the output converges to r events/sec.
 *
 * Built on [DecayingSum]. Use [DecayingEventRate] for pure event counting.
 */
class DecayingRate(
    val halfLife: Duration,
    val mode: StreamMode = defaultStreamMode,
    override val name: String? = null,
) : SeriesStat<DecayingRateResult>, HasRate {

    private val alpha = ln(2.0) / halfLife.inWholeNanoseconds.toDouble()
    private val _sum = DecayingSum(halfLife, mode)

    override fun update(value: Double, timestampNanos: Long, weight: Double) =
        _sum.update(value, timestampNanos, weight)

    override fun read(timestampNanos: Long): DecayingRateResult {
        val sum = _sum.read(timestampNanos).sum
        return DecayingRateResult(sum * alpha * 1e9, timestampNanos, name)
    }

    override fun merge(values: DecayingRateResult) {
        if (values.rate <= 0.0) return
        val sum = values.rate / (alpha * 1e9)
        _sum.merge(DecayingSumResult(sum, values.timestampNanos))
    }

    override fun reset() = _sum.reset()

    override val rate: Double get() = read().rate

    override fun create(mode: StreamMode?, name: String?) =
        DecayingRate(halfLife, mode ?: this.mode, name ?: this.name)
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
    override val name: String? = null,
) : SeriesStat<DecayingMeanResult>, HasMean {

    private val _sumX = DecayingSum(halfLife, mode)
    private val _sumW = DecayingSum(halfLife, mode)

    override val mean: Double
        get() {
            val now = currentTimeNanos()
            val sumW = _sumW.read(now).sum
            return if (sumW > 0.0) _sumX.read(now).sum / sumW else 0.0
        }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        _sumX.update(value, timestampNanos, weight)
        _sumW.update(1.0, timestampNanos, weight)
    }

    override fun read(timestampNanos: Long): DecayingMeanResult {
        val sumX = _sumX.read(timestampNanos).sum
        val sumW = _sumW.read(timestampNanos).sum
        val mean = if (sumW > 0.0) sumX / sumW else 0.0
        return DecayingMeanResult(mean, sumW, timestampNanos, name)
    }

    override fun merge(values: DecayingMeanResult) {
        if (values.decayingCount <= 0.0) return
        _sumX.merge(DecayingSumResult(values.mean * values.decayingCount, values.timestampNanos))
        _sumW.merge(DecayingSumResult(values.decayingCount, values.timestampNanos))
    }

    override fun reset() {
        _sumX.reset()
        _sumW.reset()
    }

    override fun create(mode: StreamMode?, name: String?) =
        DecayingMean(halfLife, mode ?: this.mode, name ?: this.name)
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
    override val name: String? = null,
) : SeriesStat<DecayingVarianceResult>, HasMean, HasVariance {

    private val _sumX2 = DecayingSum(halfLife, mode)
    private val _sumX = DecayingSum(halfLife, mode)
    private val _sumW = DecayingSum(halfLife, mode)

    override val mean: Double
        get() {
            val now = currentTimeNanos()
            val sumW = _sumW.read(now).sum
            return if (sumW > 0.0) _sumX.read(now).sum / sumW else 0.0
        }

    override val variance: Double
        get() {
            val now = currentTimeNanos()
            val sumW = _sumW.read(now).sum
            if (sumW <= 0.0) return 0.0
            val m = _sumX.read(now).sum / sumW
            val m2 = _sumX2.read(now).sum / sumW
            return (m2 - m * m).coerceAtLeast(0.0)
        }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        _sumX2.update(value * value, timestampNanos, weight)
        _sumX.update(value, timestampNanos, weight)
        _sumW.update(1.0, timestampNanos, weight)
    }

    override fun read(timestampNanos: Long): DecayingVarianceResult {
        val sumX2 = _sumX2.read(timestampNanos).sum
        val sumX = _sumX.read(timestampNanos).sum
        val sumW = _sumW.read(timestampNanos).sum
        val mean = if (sumW > 0.0) sumX / sumW else 0.0
        val variance = if (sumW > 0.0) (sumX2 / sumW - mean * mean).coerceAtLeast(0.0) else 0.0
        return DecayingVarianceResult(mean, variance, sumW, timestampNanos, name)
    }

    override fun merge(values: DecayingVarianceResult) {
        if (values.decayingCount <= 0.0) return
        val sumW = values.decayingCount
        val sumX = values.mean * sumW
        val sumX2 = (values.variance + values.mean * values.mean) * sumW
        _sumX2.merge(DecayingSumResult(sumX2, values.timestampNanos))
        _sumX.merge(DecayingSumResult(sumX, values.timestampNanos))
        _sumW.merge(DecayingSumResult(sumW, values.timestampNanos))
    }

    override fun reset() {
        _sumX2.reset()
        _sumX.reset()
        _sumW.reset()
    }

    override fun create(mode: StreamMode?, name: String?) =
        DecayingVariance(halfLife, mode ?: this.mode, name ?: this.name)
}
