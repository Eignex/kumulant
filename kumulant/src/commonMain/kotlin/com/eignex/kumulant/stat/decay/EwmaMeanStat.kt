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
 *
 * # Concurrency
 *
 * Under [Concurrency.Strict] and [Concurrency.HighWrite] the update body is
 * locked so individual updates apply atomically, but **the final snapshot still
 * depends on the order in which updates arrive**. The recurrence folds each
 * sample into the running mean against the previous biased mean — a different
 * interleaving of the same multiset of updates produces a different result by
 * ~1–10%. This is intrinsic to EWMA, not a bug; locking does not (and cannot)
 * recover a serial reference value.
 *
 * Under [Concurrency.Relaxed] the lock is dropped and individual updates may
 * additionally race on the read-modify-write of `biasedMean`, adding further
 * drift on top of the order dependence. Prefer [Concurrency.Strict] when
 * correctness matters more than write throughput.
 */
class EwmaMeanStat(
    /** Per-observation smoothing schedule. */
    val weighting: DecayWeighting.Alpha,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<WeightedMeanResult> {

    constructor(alpha: Double, concurrency: Concurrency = Concurrency.None) :
        this(DecayWeighting.Alpha(alpha), concurrency)

    /** Smoothing factor; larger = more weight on recent samples. */
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
