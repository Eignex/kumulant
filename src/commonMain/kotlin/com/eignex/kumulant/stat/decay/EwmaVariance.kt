package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stat.summary.WeightedVarianceResult

import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.defaultStreamMode

/**
 * Exponentially weighted moving variance driven by cumulative observation weight.
 *
 * Tracks biased mean and biased second-moment `M2` via Welford-style delta updates,
 * then divides by the bias correction at read time.
 */
class EwmaVariance(
    val weighting: DecayWeighting.Alpha,
    override val mode: StreamMode = defaultStreamMode,
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
