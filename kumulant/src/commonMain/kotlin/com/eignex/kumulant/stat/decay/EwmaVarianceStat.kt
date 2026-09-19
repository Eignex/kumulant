package com.eignex.kumulant.stat.decay

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.stat.summary.WeightedVarianceResult
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.welfordLock
import com.eignex.kumulant.stream.welfordMode

/**
 * Exponentially weighted moving variance driven by cumulative observation weight.
 *
 * Tracks biased mean and biased second-moment `M2` via Welford-style delta updates,
 * then divides by the bias correction at read time.
 *
 * **Use cases:** weight-windowed dispersion where elapsed time isn't relevant
 * but observation count is. Reach for this over [DecayingVarianceStat] when
 * the cadence is irregular; reach for
 * [com.eignex.kumulant.stat.summary.VarianceStat] if you need an
 * order-independent variance under contention.
 *
 * **Memory:** O(1); three doubles plus a lock.
 *
 * **Update:** O(1) per observation.
 *
 * **Concurrency:** Order-dependent recurrence, same as [EwmaMeanStat]. Even
 * [Concurrency.Strict] does not reproduce a serial reference value because
 * the lock serialises arrival, not order of arrival; the snapshot drifts
 * ~5–10% under contention. [Concurrency.Relaxed] additionally drops the lock.
 */
class EwmaVarianceStat(
    /** Per-observation smoothing schedule. */
    val weighting: DecayWeighting.Alpha,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<WeightedVarianceResult> {

    constructor(alpha: Double, concurrency: Concurrency = Concurrency.None) :
        this(DecayWeighting.Alpha(alpha), concurrency)

    /** Smoothing factor; larger = more weight on recent samples. */
    val alpha: Double get() = weighting.alpha

    private val mode = concurrency.welfordMode()
    private val lock = concurrency.welfordLock()
    private val biasedMean = mode.newDouble(0.0)
    private val biasedM2 = mode.newDouble(0.0)
    private val totalWeights = mode.newDouble(0.0)

    private val mean: Double
        get() {
            return weighting.debias(biasedMean.load(), totalWeights.load())
        }

    private val variance: Double
        get() {
            return weighting.debias(biasedM2.load(), totalWeights.load())
        }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        lock.guarded {
            updateLocked(value, weight)
        }
    }

    private fun updateLocked(value: Double, weight: Double) {
        val a = weighting.correction(weight)

        val currentRawMean = biasedMean.load()
        val increment = a * (value - currentRawMean)
        val newRawMean = currentRawMean + increment

        // Centred on the debiased means, never the raw accumulators: those still carry the zero seed
        // they started from, which would enter the variance as a term proportional to mean^2 and only
        // decay at the smoothing rate. read() debiases M2, so the two sides have to agree.
        val oldWeight = totalWeights.load()
        val oldMean = weighting.debias(currentRawMean, oldWeight)
        val newMean = weighting.debias(newRawMean, oldWeight + weight)

        val currentBiasedM2 = biasedM2.load()
        biasedM2.add(a * ((value - oldMean) * (value - newMean) - currentBiasedM2))
        biasedMean.add(increment)
        totalWeights.add(weight)
    }

    override fun merge(values: WeightedVarianceResult) = lock.guarded {
        val remoteWeightRaw = values.totalWeights
        if (remoteWeightRaw <= 0.0) return@guarded

        val localWeightRaw = totalWeights.load()
        val w1 = weighting.correction(localWeightRaw)
        val w2 = weighting.correction(remoteWeightRaw)
        val wSum = w1 + w2

        if (wSum == 0.0) return@guarded

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

    override fun reset() = lock.guarded {
        biasedMean.store(0.0)
        biasedM2.store(0.0)
        totalWeights.store(0.0)
    }

    override fun read(timestampNanos: Long) = lock.guarded {
        WeightedVarianceResult(
            totalWeights.load(),
            mean,
            variance,
        )
    }

    override fun create(concurrency: Concurrency?) = EwmaVarianceStat(weighting, concurrency ?: this.concurrency)
}
