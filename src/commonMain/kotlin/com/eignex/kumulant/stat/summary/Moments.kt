package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.HasSampleVariance
import com.eignex.kumulant.core.HasShapeMoments
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.defaultStreamMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** First four central moments (m2..m4) plus mean and total weight. */
@Serializable
@SerialName("Moments")
data class MomentsResult(
    override val totalWeights: Double,
    val mean: Double,
    override val m2: Double,
    override val m3: Double,
    override val m4: Double
) : Result, HasSampleVariance, HasShapeMoments {
    override val sst: Double get() = m2
}

/**
 * Weighted first four central moments (mean, m2, m3, m4) for skewness and kurtosis.
 *
 * Uses the Pébay/Welford parallel recurrences; suitable for streaming and merge.
 */
class Moments(
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<MomentsResult> {

    private val totalWeights = mode.newDouble(0.0)
    private val mean = mode.newDouble(0.0)
    private val m2 = mode.newDouble(0.0)
    private val m3 = mode.newDouble(0.0)
    private val m4 = mode.newDouble(0.0)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0) return

        val oldM1 = mean.load()
        val oldM2 = m2.load()
        val oldM3 = m3.load()
        val nextW = totalWeights.addAndGet(weight)
        val oldW = nextW - weight

        val delta = value - oldM1
        val deltaW = delta * (weight / nextW)
        val deltaW2 = deltaW * deltaW
        val term1 = delta * deltaW * oldW

        m4.add(term1 * deltaW2 * (nextW * nextW - 3 * nextW + 3) + 6 * deltaW2 * oldM2 - 4 * deltaW * oldM3)
        m3.add(term1 * deltaW * (nextW - 2) - 3 * deltaW * oldM2)
        m2.add(term1)
        mean.add(deltaW)
    }

    override fun merge(values: MomentsResult) {
        val w1 = totalWeights.load()
        val w2 = values.totalWeights
        if (w2 <= 0.0) return

        val m1 = mean.load()
        val m2Local = m2.load()
        val m3Local = m3.load()

        val nextW = w1 + w2
        val delta = values.mean - m1

        val delta2 = delta * delta
        val delta3 = delta2 * delta
        val delta4 = delta3 * delta

        val nextWSq = nextW * nextW
        val nextWCu = nextWSq * nextW
        // Incremental M3 using incoming raw m2/m3
        val m3Delta = values.m3 +
            delta3 * (w1 * w2 * (w1 - w2) / nextWSq) +
            3.0 * delta * (w1 * values.m2 - w2 * m2Local) / nextW

        // Incremental M4 using incoming raw m2/m3/m4
        val m4Delta = values.m4 +
            delta4 * (w1 * w2 * (w1 * w1 - w1 * w2 + w2 * w2) / nextWCu) +
            6.0 * delta2 * (w1 * w1 * values.m2 + w2 * w2 * m2Local) / nextWSq +
            4.0 * delta * (w1 * values.m3 - w2 * m3Local) / nextW

        m4.add(m4Delta)
        m3.add(m3Delta)
        m2.add(values.m2 + (delta2 * w1 * w2 / nextW))
        mean.add(delta * (w2 / nextW))
        totalWeights.add(w2)
    }

    override fun reset() {
        totalWeights.store(0.0)
        mean.store(0.0)
        m2.store(0.0)
        m3.store(0.0)
        m4.store(0.0)
    }

    override fun read(timestampNanos: Long) =
        MomentsResult(
            totalWeights.load(),
            mean.load(),
            m2.load(),
            m3.load(),
            m4.load()
        )

    override fun create(mode: StreamMode?) = Moments(mode ?: this.mode)
}
