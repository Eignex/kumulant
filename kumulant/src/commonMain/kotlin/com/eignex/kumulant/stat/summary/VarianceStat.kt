package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasCenterScale
import com.eignex.kumulant.core.HasSampleVariance
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.core.requireLiveWeight
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.welfordLock
import com.eignex.kumulant.stream.welfordMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Mean and population variance. */
@Serializable
@SerialName("VarianceResult")
data class VarianceResult(
    /** Running arithmetic mean. */
    val mean: Double,
    /** Population variance: `Sum (x - mean)^2 * w / totalWeights`. */
    val variance: Double,
) : Result

/** Weighted mean and variance with [totalWeights] for merge arithmetic. */
@Serializable
@SerialName("WeightedVarianceResult")
data class WeightedVarianceResult(
    override val totalWeights: Double,
    /** Weighted running mean. */
    val mean: Double,
    override val variance: Double,
) : Result,
    HasSampleVariance,
    HasCenterScale {
    override val center: Double get() = mean
    override val scale: Double get() = stdDev
}

/**
 * Weighted mean and variance via Welford with Chan-style parallel merge.
 *
 * Population variance `sst / totalWeights`; use [HasSampleVariance.sampleVariance]
 * on the result for the unbiased estimator.
 *
 * **Use cases:** dispersion of any scalar quantity; the standard ingredient
 * for control charts, anomaly thresholds, and bandit posteriors. Pairs with
 * [MomentsStat] when skewness/kurtosis are also needed.
 *
 * **Weights:** zero is a no-op; a negative weight downdates. See [MeanStat] for
 * the shared contract. The `sst` recurrence inverts exactly: removing `(x, w)`
 * from total `W` subtracts `W * w * delta^2 / (W - w)`, which is algebraically
 * the same quantity the forward step added.
 *
 * **Memory:** O(1); three doubles plus a lock.
 *
 * **Update:** O(1) per observation.
 *
 * **Concurrency:** Welford-coupled cells. [Concurrency.Strict] and
 * [Concurrency.HighWrite] lock the body; exact match to a serial run up to
 * floating-point reorder ULPs. [Concurrency.Relaxed] drops the lock and the
 * three cells race independently; the variance drifts ~1e-4 relative under
 * contention but never throws.
 */
class VarianceStat(override val concurrency: Concurrency = Concurrency.None) : SeriesStat<WeightedVarianceResult> {

    private val mode = concurrency.welfordMode()
    private val lock = concurrency.welfordLock()
    private val totalWeights = mode.newDouble(0.0)
    private val mean = mode.newDouble(0.0)
    private val sst = mode.newDouble(0.0)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        lock.guarded {
            val priorW = totalWeights.load()
            requireLiveWeight(priorW, weight)
            val nextW = totalWeights.addAndGet(weight)
            val delta = value - mean.load()
            val r = delta * (weight / nextW)
            mean.add(r)
            sst.add(priorW * delta * r)
        }
    }

    override fun merge(values: WeightedVarianceResult, workspace: com.eignex.koblas.Workspace?) = lock.guarded {
        if (values.totalWeights <= 0.0) return@guarded
        val w1 = totalWeights.load()
        val w2 = values.totalWeights
        val nextW = totalWeights.addAndGet(w2)
        val delta = values.mean - mean.load()
        mean.add(delta * (w2 / nextW))
        sst.add(values.variance * w2 + (delta * delta) * (w1 * w2 / nextW))
    }

    override fun reset() = lock.guarded {
        totalWeights.store(0.0)
        mean.store(0.0)
        sst.store(0.0)
    }

    override fun read(timestampNanos: Long): WeightedVarianceResult = lock.guarded {
        val w = totalWeights.load()
        val m = mean.load()
        val s = sst.load()
        val variance = if (w > 0.0) s / w else 0.0
        WeightedVarianceResult(w, m, variance)
    }

    override fun create(concurrency: Concurrency?) = VarianceStat(concurrency ?: this.concurrency)
}
