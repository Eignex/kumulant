package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stat.summary.WeightedMeanResult

import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.defaultStreamMode

// Event-weight-decayed family (Alpha weighting).
// Bias-corrected exponential moving average/variance driven by cumulative observation
// weight rather than wall-clock time. See [DecayWeighting.Alpha].

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
            val w = totalWeights.load()
            if (w == 0.0) return 0.0
            val correction = weighting.correction(w)
            return if (correction == 0.0) Double.NaN else biased / correction
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
