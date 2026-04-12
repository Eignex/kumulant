package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.WeightedMeanResult
import com.eignex.kumulant.core.WeightedVarianceResult
import kotlin.math.exp

/**
 * Exponentially weighted moving average (observation-count based).
 *
 * Each new observation is blended in with weight [alpha]; the running
 * estimate decays as `(1 − alpha)^n` over n observations. Time plays no
 * role — decay is driven by the number of updates, not wall-clock elapsed.
 *
 * Use [DecayingMean] instead when observations arrive at irregular intervals
 * and you want decay anchored to real time.
 */
class EwmaMean(
    val alpha: Double,
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<WeightedMeanResult> {

    private val biasedMean = mode.newDouble(0.0)
    private val totalWeights = mode.newDouble(0.0)

    private fun getCorrection(weight: Double): Double {
        return if (weight == 0.0) 0.0 else 1.0 - exp(-alpha * weight)
    }

    private val mean: Double
        get() {
            val biased = biasedMean.load()
            val weight = totalWeights.load()
            val correction = getCorrection(weight)
            return if (correction == 0.0) 0.0 else biased / correction
        }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        val a = getCorrection(weight)
        biasedMean.add(a * (value - biasedMean.load()))
        totalWeights.add(weight)
    }

    override fun merge(values: WeightedMeanResult) {
        val localMean = this.mean
        val localWeight = totalWeights.load()
        val localEffectiveWeight = getCorrection(localWeight)

        val remoteMean = values.mean
        val remoteWeight = values.totalWeights
        val remoteEffectiveWeight = getCorrection(remoteWeight)

        val totalEffectiveWeight = localEffectiveWeight + remoteEffectiveWeight
        if (totalEffectiveWeight == 0.0) return

        val mergedMean =
            (localMean * localEffectiveWeight + remoteMean * remoteEffectiveWeight) / totalEffectiveWeight

        val newTotalWeight = localWeight + remoteWeight
        val newCorrection = getCorrection(newTotalWeight)
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

    override fun create(mode: StreamMode?) = EwmaMean(alpha, mode ?: this.mode)
}

/**
 * Exponentially weighted moving variance (observation-count based).
 *
 * Tracks both mean and variance using EWMA with bias correction. Decay
 * is driven by the number of updates, not elapsed time.
 *
 * Use [DecayingVariance] instead when you want time-anchored decay.
 */
class EwmaVariance(
    val alpha: Double,
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<WeightedVarianceResult> {

    private val biasedMean = mode.newDouble(0.0)
    private val biasedM2 = mode.newDouble(0.0)
    private val totalWeights = mode.newDouble(0.0)

    private fun getCorrection(weight: Double): Double {
        return if (weight == 0.0) 0.0 else 1.0 - exp(-alpha * weight)
    }

    private val mean: Double
        get() {
            val biased = biasedMean.load()
            val weight = totalWeights.load()
            val correction = getCorrection(weight)
            return if (correction == 0.0) 0.0 else biased / correction
        }

    private val variance: Double
        get() {
            val biasedVar = biasedM2.load()
            val weight = totalWeights.load()
            val correction = getCorrection(weight)
            return if (correction == 0.0) 0.0 else biasedVar / correction
        }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        val a = getCorrection(weight)

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
        val w1 = getCorrection(localWeightRaw)
        val w2 = getCorrection(remoteWeightRaw)
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
        val newCorrection = getCorrection(newTotalWeight)

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

    override fun create(mode: StreamMode?) = EwmaVariance(alpha, mode ?: this.mode)
}
