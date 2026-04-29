package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.HasSampleVariance
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.StreamRef
import com.eignex.kumulant.stream.defaultStreamMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Mean and population variance. */
@Serializable
@SerialName("Variance")
data class VarianceResult(
    val mean: Double,
    val variance: Double
) : Result

/** Weighted mean and variance with [totalWeights] for merge arithmetic. */
@Serializable
@SerialName("WeightedVariance")
data class WeightedVarianceResult(
    override val totalWeights: Double,
    val mean: Double,
    override val variance: Double
) : Result, HasSampleVariance

/**
 * Weighted mean and variance via Welford with Chan-style parallel merge.
 *
 * Population variance `sst / totalWeights`; use [HasSampleVariance.sampleVariance] on
 * the result for the unbiased estimator. State is held as a single immutable snapshot
 * and CAS-swapped, so concurrent updates under [com.eignex.kumulant.stream.AtomicMode]
 * are atomic and reads observe a consistent triple.
 */
class Variance(
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<WeightedVarianceResult> {

    private data class State(val totalWeights: Double, val mean: Double, val sst: Double)

    private val stateRef: StreamRef<State> = mode.newReference(State(0.0, 0.0, 0.0))

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight == 0.0) return
        while (true) {
            val s = stateRef.load()
            val nextW = s.totalWeights + weight
            val delta = value - s.mean
            val r = delta * (weight / nextW)
            val nextMean = s.mean + r
            val nextSst = s.sst + s.totalWeights * delta * r
            if (stateRef.compareAndSet(s, State(nextW, nextMean, nextSst))) return
        }
    }

    override fun merge(values: WeightedVarianceResult) {
        if (values.totalWeights <= 0.0) return
        val sst2 = values.variance * values.totalWeights
        while (true) {
            val s = stateRef.load()
            val w1 = s.totalWeights
            val w2 = values.totalWeights
            val nextW = w1 + w2
            val delta = values.mean - s.mean
            val nextMean = s.mean + delta * (w2 / nextW)
            val nextSst = s.sst + sst2 + (delta * delta) * (w1 * w2 / nextW)
            if (stateRef.compareAndSet(s, State(nextW, nextMean, nextSst))) return
        }
    }

    override fun reset() {
        stateRef.store(State(0.0, 0.0, 0.0))
    }

    override fun read(timestampNanos: Long): WeightedVarianceResult {
        val s = stateRef.load()
        val variance = if (s.totalWeights > 0.0) s.sst / s.totalWeights else 0.0
        return WeightedVarianceResult(s.totalWeights, s.mean, variance)
    }

    override fun create(mode: StreamMode?) = Variance(mode ?: this.mode)
}
