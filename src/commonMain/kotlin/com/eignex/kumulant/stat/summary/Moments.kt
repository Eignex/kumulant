package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.HasSampleVariance
import com.eignex.kumulant.core.HasShapeMoments
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.StreamRef
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
 * State is held as a single immutable snapshot and CAS-swapped, so concurrent updates
 * under [com.eignex.kumulant.stream.AtomicMode] are atomic.
 */
class Moments(
    override val mode: StreamMode = defaultStreamMode,
) : SeriesStat<MomentsResult> {

    private data class State(
        val totalWeights: Double,
        val mean: Double,
        val m2: Double,
        val m3: Double,
        val m4: Double,
    )

    private val stateRef: StreamRef<State> = mode.newReference(State(0.0, 0.0, 0.0, 0.0, 0.0))

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0) return
        while (true) {
            val s = stateRef.load()
            val nextW = s.totalWeights + weight
            val oldW = s.totalWeights

            val delta = value - s.mean
            val deltaW = delta * (weight / nextW)
            val deltaW2 = deltaW * deltaW
            val term1 = delta * deltaW * oldW

            val nextM4 = s.m4 +
                term1 * deltaW2 * (nextW * nextW - 3 * nextW + 3) +
                6 * deltaW2 * s.m2 -
                4 * deltaW * s.m3
            val nextM3 = s.m3 + term1 * deltaW * (nextW - 2) - 3 * deltaW * s.m2
            val nextM2 = s.m2 + term1
            val nextMean = s.mean + deltaW

            if (stateRef.compareAndSet(s, State(nextW, nextMean, nextM2, nextM3, nextM4))) return
        }
    }

    override fun merge(values: MomentsResult) {
        if (values.totalWeights <= 0.0) return
        while (true) {
            val s = stateRef.load()
            val w1 = s.totalWeights
            val w2 = values.totalWeights

            val nextW = w1 + w2
            val delta = values.mean - s.mean

            val delta2 = delta * delta
            val delta3 = delta2 * delta
            val delta4 = delta3 * delta

            val nextWSq = nextW * nextW
            val nextWCu = nextWSq * nextW

            val m3Delta = values.m3 +
                delta3 * (w1 * w2 * (w1 - w2) / nextWSq) +
                3.0 * delta * (w1 * values.m2 - w2 * s.m2) / nextW

            val m4Delta = values.m4 +
                delta4 * (w1 * w2 * (w1 * w1 - w1 * w2 + w2 * w2) / nextWCu) +
                6.0 * delta2 * (w1 * w1 * values.m2 + w2 * w2 * s.m2) / nextWSq +
                4.0 * delta * (w1 * values.m3 - w2 * s.m3) / nextW

            val nextM4 = s.m4 + m4Delta
            val nextM3 = s.m3 + m3Delta
            val nextM2 = s.m2 + values.m2 + (delta2 * w1 * w2 / nextW)
            val nextMean = s.mean + delta * (w2 / nextW)

            if (stateRef.compareAndSet(s, State(nextW, nextMean, nextM2, nextM3, nextM4))) return
        }
    }

    override fun reset() {
        stateRef.store(State(0.0, 0.0, 0.0, 0.0, 0.0))
    }

    override fun read(timestampNanos: Long): MomentsResult {
        val s = stateRef.load()
        return MomentsResult(s.totalWeights, s.mean, s.m2, s.m3, s.m4)
    }

    override fun create(mode: StreamMode?) = Moments(mode ?: this.mode)
}
