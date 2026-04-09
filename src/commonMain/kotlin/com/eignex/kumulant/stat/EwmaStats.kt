package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.concurrent.getValue
import com.eignex.kumulant.core.HasMean
import com.eignex.kumulant.core.HasTotalWeights
import com.eignex.kumulant.core.HasVariance
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
    override val name: String? = null
) : SeriesStat<WeightedMeanResult>, HasMean, HasTotalWeights {

    private val _biasedMean = mode.newDouble(0.0)
    private val _totalWeights = mode.newDouble(0.0)

    private fun getCorrection(weight: Double): Double {
        return if (weight == 0.0) 0.0 else 1.0 - exp(-alpha * weight)
    }

    override val totalWeights: Double by _totalWeights
    override val mean: Double
        get() {
            val biased = _biasedMean.load()
            val weight = _totalWeights.load()
            val correction = getCorrection(weight)
            return if (correction == 0.0) 0.0 else biased / correction
        }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        val a = getCorrection(weight)
        _biasedMean.add(a * (value - _biasedMean.load()))
        _totalWeights.add(weight)
    }

    override fun merge(values: WeightedMeanResult) {
        val localMean = this.mean
        val localWeight = _totalWeights.load()
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

        _biasedMean.add(targetBiasedMean - _biasedMean.load())
        _totalWeights.add(remoteWeight)
    }

    override fun reset() {
        _biasedMean.store(0.0)
        _totalWeights.store(0.0)
    }

    override fun read(timestampNanos: Long) =
        WeightedMeanResult(totalWeights, mean, name)

    override fun create(mode: StreamMode?, name: String?) = EwmaMean(alpha, mode ?: this.mode, name ?: this.name)
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
    override val name: String? = null
) : SeriesStat<WeightedVarianceResult>, HasMean, HasVariance, HasTotalWeights {

    private val _biasedMean = mode.newDouble(0.0)
    private val _biasedM2 = mode.newDouble(0.0)
    private val _totalWeights = mode.newDouble(0.0)

    private fun getCorrection(weight: Double): Double {
        return if (weight == 0.0) 0.0 else 1.0 - exp(-alpha * weight)
    }

    override val totalWeights: Double by _totalWeights

    override val mean: Double
        get() {
            val biased = _biasedMean.load()
            val weight = _totalWeights.load()
            val correction = getCorrection(weight)
            return if (correction == 0.0) 0.0 else biased / correction
        }

    override val variance: Double
        get() {
            val biasedVar = _biasedM2.load()
            val weight = _totalWeights.load()
            val correction = getCorrection(weight)
            return if (correction == 0.0) 0.0 else biasedVar / correction
        }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        val a = getCorrection(weight)

        val currentRawMean = _biasedMean.load()
        val delta = value - currentRawMean
        val increment = a * delta
        val newRawMean = currentRawMean + increment

        val currentBiasedM2 = _biasedM2.load()
        _biasedM2.add(a * (delta * (value - newRawMean) - currentBiasedM2))
        _biasedMean.add(increment)
        _totalWeights.add(weight)
    }

    override fun merge(values: WeightedVarianceResult) {
        val remoteWeightRaw = values.totalWeights
        if (remoteWeightRaw <= 0.0) return

        val localWeightRaw = _totalWeights.load()
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

        _biasedMean.add(mergedMean * newCorrection - _biasedMean.load())
        _biasedM2.add(mergedVariance * newCorrection - _biasedM2.load())
        _totalWeights.add(remoteWeightRaw)
    }

    override fun reset() {
        _biasedMean.store(0.0)
        _biasedM2.store(0.0)
        _totalWeights.store(0.0)
    }

    override fun read(timestampNanos: Long) = WeightedVarianceResult(
        totalWeights,
        mean,
        variance,
        name
    )

    override fun create(mode: StreamMode?, name: String?) = EwmaVariance(alpha, mode ?: this.mode, name ?: this.name)
}
