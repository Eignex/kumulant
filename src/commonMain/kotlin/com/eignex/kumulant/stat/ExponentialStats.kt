package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.DecayWeighting
import com.eignex.kumulant.concurrent.StreamDouble
import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.currentTimeNanos
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.core.DecayingMeanResult
import com.eignex.kumulant.core.DecayingSumResult
import com.eignex.kumulant.core.DecayingVarianceResult
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.WeightedMeanResult
import com.eignex.kumulant.core.WeightedVarianceResult
import kotlin.math.exp
import kotlin.time.Duration

// -----------------------------------------------------------------------------
// Time-decayed family (HalfLife weighting).
//
// S(t) = Σ vᵢ · wᵢ · exp(−α·(t − tᵢ)) with α = ln(2)/halfLife. Decay advances with
// wall-clock time regardless of event frequency. See [DecayWeighting.HalfLife].
// -----------------------------------------------------------------------------

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
        DecayingMean(weighting, mode ?: this.mode)
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
        DecayingVariance(weighting, mode ?: this.mode)
}

// -----------------------------------------------------------------------------
// Event-weight-decayed family (Alpha weighting).
//
// Bias-corrected exponential moving average/variance driven by cumulative observation
// weight rather than wall-clock time. See [DecayWeighting.Alpha].
// -----------------------------------------------------------------------------

/**
 * Exponentially weighted moving average driven by cumulative observation weight.
 *
 * Uses the biased-mean formulation: `biasedMean ← biasedMean + a·(value − biasedMean)`
 * with `a = 1 − exp(−α·w)`. Read returns the bias-corrected value
 * `biasedMean / (1 − exp(−α·totalWeights))`.
 */
class EwmaMean(
    val weighting: DecayWeighting.Alpha,
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<WeightedMeanResult> {

    constructor(alpha: Double, mode: StreamMode = defaultStreamMode) :
        this(DecayWeighting.Alpha(alpha), mode)

    val alpha: Double get() = weighting.alpha

    private val biasedMean = mode.newDouble(0.0)
    private val totalWeights = mode.newDouble(0.0)

    private val mean: Double
        get() {
            val biased = biasedMean.load()
            val correction = weighting.correction(totalWeights.load())
            return if (correction == 0.0) 0.0 else biased / correction
        }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        val a = weighting.correction(weight)
        biasedMean.add(a * (value - biasedMean.load()))
        totalWeights.add(weight)
    }

    override fun merge(values: WeightedMeanResult) {
        val localMean = this.mean
        val localWeight = totalWeights.load()
        val localEffectiveWeight = weighting.correction(localWeight)

        val remoteMean = values.mean
        val remoteWeight = values.totalWeights
        val remoteEffectiveWeight = weighting.correction(remoteWeight)

        val totalEffectiveWeight = localEffectiveWeight + remoteEffectiveWeight
        if (totalEffectiveWeight == 0.0) return

        val mergedMean =
            (localMean * localEffectiveWeight + remoteMean * remoteEffectiveWeight) / totalEffectiveWeight

        val newTotalWeight = localWeight + remoteWeight
        val newCorrection = weighting.correction(newTotalWeight)
        val targetBiasedMean = mergedMean * newCorrection

        biasedMean.add(targetBiasedMean - biasedMean.load())
        totalWeights.add(remoteWeight)
    }

    override fun reset() {
        biasedMean.store(0.0)
        totalWeights.store(0.0)
    }

    override fun read(timestampNanos: Long) =
        WeightedMeanResult(totalWeights.load(), mean)

    override fun create(mode: StreamMode?) = EwmaMean(weighting, mode ?: this.mode)
}

/**
 * Exponentially weighted moving variance driven by cumulative observation weight.
 *
 * Tracks biased mean and biased second-moment `M2` via Welford-style delta updates,
 * then divides by the bias correction at read time.
 */
class EwmaVariance(
    val weighting: DecayWeighting.Alpha,
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<WeightedVarianceResult> {

    constructor(alpha: Double, mode: StreamMode = defaultStreamMode) :
        this(DecayWeighting.Alpha(alpha), mode)

    val alpha: Double get() = weighting.alpha

    private val biasedMean = mode.newDouble(0.0)
    private val biasedM2 = mode.newDouble(0.0)
    private val totalWeights = mode.newDouble(0.0)

    private val mean: Double
        get() {
            val biased = biasedMean.load()
            val correction = weighting.correction(totalWeights.load())
            return if (correction == 0.0) 0.0 else biased / correction
        }

    private val variance: Double
        get() {
            val biasedVar = biasedM2.load()
            val correction = weighting.correction(totalWeights.load())
            return if (correction == 0.0) 0.0 else biasedVar / correction
        }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        val a = weighting.correction(weight)

        val currentRawMean = biasedMean.load()
        val delta = value - currentRawMean
        val increment = a * delta
        val newRawMean = currentRawMean + increment

        val currentBiasedM2 = biasedM2.load()
        biasedM2.add(a * (delta * (value - newRawMean) - currentBiasedM2))
        biasedMean.add(increment)
        totalWeights.add(weight)
    }

    override fun merge(values: WeightedVarianceResult) {
        val remoteWeightRaw = values.totalWeights
        if (remoteWeightRaw <= 0.0) return

        val localWeightRaw = totalWeights.load()
        val w1 = weighting.correction(localWeightRaw)
        val w2 = weighting.correction(remoteWeightRaw)
        val wSum = w1 + w2

        if (wSum == 0.0) return

        val localMean = this.mean
        val localVar = this.variance
        val remoteMean = values.mean
        val remoteVar = values.variance

        val mergedMean = (localMean * w1 + remoteMean * w2) / wSum
        val deltaMean = localMean - remoteMean
        val mergedVariance = ((w1 * localVar) + (w2 * remoteVar) + (w1 * w2 * deltaMean * deltaMean) / wSum) / wSum

        val newTotalWeight = localWeightRaw + remoteWeightRaw
        val newCorrection = weighting.correction(newTotalWeight)

        biasedMean.add(mergedMean * newCorrection - biasedMean.load())
        biasedM2.add(mergedVariance * newCorrection - biasedM2.load())
        totalWeights.add(remoteWeightRaw)
    }

    override fun reset() {
        biasedMean.store(0.0)
        biasedM2.store(0.0)
        totalWeights.store(0.0)
    }

    override fun read(timestampNanos: Long) = WeightedVarianceResult(
        totalWeights.load(),
        mean,
        variance
    )

    override fun create(mode: StreamMode?) = EwmaVariance(weighting, mode ?: this.mode)
}
