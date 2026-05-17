package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stat.summary.WeightedMeanResult
import com.eignex.kumulant.stream.welfordLock
import com.eignex.kumulant.stream.welfordMode

/**
 * Exponentially weighted moving average driven by cumulative observation weight.
 *
 * Uses the biased-mean formulation: `biasedMean = biasedMean + a*(value - biasedMean)`
 * with `a = 1 - exp(-alpha*w)`. Read returns the bias-corrected value
 * `biasedMean / (1 - exp(-alpha*totalWeights))`.
 */
class EwmaMeanStat(
    val weighting: DecayWeighting.Alpha,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<WeightedMeanResult> {

    constructor(alpha: Double, concurrency: Concurrency = Concurrency.None) :
        this(DecayWeighting.Alpha(alpha), concurrency)

    val alpha: Double get() = weighting.alpha

    private val mode = concurrency.welfordMode()
    private val lock = concurrency.welfordLock()
    private val biasedMean = mode.newDouble(0.0)
    private val totalWeights = mode.newDouble(0.0)

    private val mean: Double
        get() {
            val biased = biasedMean.load()
            val w = totalWeights.load()
            if (w == 0.0) return 0.0
            val correction = weighting.correction(w)
            return if (correction == 0.0) Double.NaN else biased / correction
        }

    override fun update(value: Double, timestampNanos: Long, weight: Double) = lock.withLock {
        val a = weighting.correction(weight)
        biasedMean.add(a * (value - biasedMean.load()))
        totalWeights.add(weight)
    }

    override fun merge(values: WeightedMeanResult) = lock.withLock {
        val localMean = this.mean
        val localWeight = totalWeights.load()
        val localEffectiveWeight = weighting.correction(localWeight)

        val remoteMean = values.mean
        val remoteWeight = values.totalWeights
        val remoteEffectiveWeight = weighting.correction(remoteWeight)

        val totalEffectiveWeight = localEffectiveWeight + remoteEffectiveWeight
        if (totalEffectiveWeight == 0.0) return@withLock

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

    override fun read(timestampNanos: Long) = lock.withLock {
        WeightedMeanResult(totalWeights.load(), mean)
    }

    override fun create(concurrency: Concurrency?) = EwmaMeanStat(weighting, concurrency ?: this.concurrency)
}
