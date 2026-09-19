package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasCenterScale
import com.eignex.kumulant.core.HasSampleVariance
import com.eignex.kumulant.core.HasShapeMoments
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.core.requireLiveWeight
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.welfordLock
import com.eignex.kumulant.stream.welfordMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** First four central moments (m2..m4) plus mean and total weight. */
@Serializable
@SerialName("MomentsResult")
data class MomentsResult(
    override val totalWeights: Double,
    /** Weighted running mean. */
    val mean: Double,
    override val m2: Double,
    override val m3: Double,
    override val m4: Double,
) : Result,
    HasSampleVariance,
    HasShapeMoments,
    HasCenterScale {
    override val sst: Double get() = m2
    override val center: Double get() = mean
    override val scale: Double get() = stdDev
}

/**
 * Weighted first four central moments (mean, m2, m3, m4) for skewness and kurtosis.
 *
 * Uses the Pebay/Welford parallel recurrences; suitable for streaming and merge.
 *
 * **Use cases:** distribution shape monitoring (skew/kurt anomaly detection,
 * non-Gaussian tail diagnostics). Heavier than [VarianceStat]; reach for it
 * only when third/fourth moments are needed.
 *
 * **Weights:** the recurrences are the fully weighted Pebay forms, not the
 * unit-weight specialisations, so any weight is supported. Zero is a no-op. A
 * negative weight removes a previously folded-in observation and inverts all four
 * moments exactly; see [MeanStat] for the shared downdate contract.
 *
 * **Memory:** O(1); five doubles plus a lock.
 *
 * **Update:** O(1) per observation.
 *
 * **Concurrency:** Welford-coupled cells. [Concurrency.Strict] and
 * [Concurrency.HighWrite] lock the body; exact match to a serial run up to
 * floating-point reorder ULPs. [Concurrency.Relaxed] drops the lock; the
 * higher-order moments drift ~1e-4 relative under contention but never throw.
 */
class MomentsStat(override val concurrency: Concurrency = Concurrency.None) : SeriesStat<MomentsResult> {

    private val mode = concurrency.welfordMode()
    private val lock = concurrency.welfordLock()
    private val totalWeights = mode.newDouble(0.0)
    private val mean = mode.newDouble(0.0)
    private val m2 = mode.newDouble(0.0)
    private val m3 = mode.newDouble(0.0)
    private val m4 = mode.newDouble(0.0)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        lock.guarded {
            val oldW = totalWeights.load()
            requireLiveWeight(oldW, weight)
            val nextW = totalWeights.addAndGet(weight)

            val priorMean = mean.load()
            val priorM2 = m2.load()
            val priorM3 = m3.load()

            val delta = value - priorMean
            val deltaW = delta * (weight / nextW)
            val deltaW2 = deltaW * deltaW
            val term1 = delta * deltaW * oldW

            // Weighted Pebay recurrences: the leading coefficients are the merge formulas
            // below specialised to a second sample of weight `weight` and zero moments.
            // They reduce to the familiar (n - 2) and (n^2 - 3n + 3) only at weight == 1.
            // Written as delta/nextW rather than deltaW/weight so a tiny weight doesn't
            // divide out.
            val m4Delta = term1 * delta * delta * (oldW * oldW - oldW * weight + weight * weight) / (nextW * nextW) +
                6 * deltaW2 * priorM2 -
                4 * deltaW * priorM3
            val m3Delta = term1 * delta * (oldW - weight) / nextW - 3 * deltaW * priorM2

            m4.add(m4Delta)
            m3.add(m3Delta)
            m2.add(term1)
            mean.add(deltaW)
        }
    }

    override fun merge(values: MomentsResult) = lock.guarded {
        if (values.totalWeights <= 0.0) return@guarded
        val w1 = totalWeights.load()
        val w2 = values.totalWeights
        val nextW = totalWeights.addAndGet(w2)

        val priorMean = mean.load()
        val priorM2 = m2.load()
        val priorM3 = m3.load()

        val delta = values.mean - priorMean
        val delta2 = delta * delta
        val delta3 = delta2 * delta
        val delta4 = delta3 * delta

        val nextWSq = nextW * nextW
        val nextWCu = nextWSq * nextW

        val m3Delta = values.m3 +
            delta3 * (w1 * w2 * (w1 - w2) / nextWSq) +
            3.0 * delta * (w1 * values.m2 - w2 * priorM2) / nextW

        val m4Delta = values.m4 +
            delta4 * (w1 * w2 * (w1 * w1 - w1 * w2 + w2 * w2) / nextWCu) +
            6.0 * delta2 * (w1 * w1 * values.m2 + w2 * w2 * priorM2) / nextWSq +
            4.0 * delta * (w1 * values.m3 - w2 * priorM3) / nextW

        m4.add(m4Delta)
        m3.add(m3Delta)
        m2.add(values.m2 + (delta2 * w1 * w2 / nextW))
        mean.add(delta * (w2 / nextW))
    }

    override fun reset() = lock.guarded {
        totalWeights.store(0.0)
        mean.store(0.0)
        m2.store(0.0)
        m3.store(0.0)
        m4.store(0.0)
    }

    override fun read(timestampNanos: Long): MomentsResult = lock.guarded {
        MomentsResult(totalWeights.load(), mean.load(), m2.load(), m3.load(), m4.load())
    }

    override fun create(concurrency: Concurrency?) = MomentsStat(concurrency ?: this.concurrency)
}
