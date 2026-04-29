package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.StreamRef
import com.eignex.kumulant.stream.defaultStreamMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Arithmetic mean. */
@Serializable
@SerialName("Mean")
data class MeanResult(
    val mean: Double,
) : Result

/** Weighted mean and accumulated weight. */
@Serializable
@SerialName("WeightedMean")
data class WeightedMeanResult(
    val totalWeights: Double,
    val mean: Double,
) : Result

/**
 * Weighted arithmetic mean via Welford-style online update.
 *
 * Numerically stable across wide dynamic ranges; merges two [Mean]s using Chan's
 * parallel algorithm. State is held as a single immutable snapshot and CAS-swapped,
 * so concurrent updates under [com.eignex.kumulant.stream.AtomicMode] are atomic.
 */
class Mean(
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<WeightedMeanResult> {

    private data class State(val totalWeights: Double, val mean: Double)

    private val stateRef: StreamRef<State> = mode.newReference(State(0.0, 0.0))

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight == 0.0) return
        while (true) {
            val s = stateRef.load()
            val nextW = s.totalWeights + weight
            val delta = value - s.mean
            val nextMean = s.mean + delta * (weight / nextW)
            if (stateRef.compareAndSet(s, State(nextW, nextMean))) return
        }
    }

    override fun read(timestampNanos: Long): WeightedMeanResult {
        val s = stateRef.load()
        return WeightedMeanResult(s.totalWeights, s.mean)
    }

    override fun merge(values: WeightedMeanResult) {
        if (values.totalWeights <= 0.0) return
        while (true) {
            val s = stateRef.load()
            val nextW = s.totalWeights + values.totalWeights
            val delta = values.mean - s.mean
            val nextMean = s.mean + delta * (values.totalWeights / nextW)
            if (stateRef.compareAndSet(s, State(nextW, nextMean))) return
        }
    }

    override fun reset() {
        stateRef.store(State(0.0, 0.0))
    }

    override fun create(mode: StreamMode?) = Mean(mode ?: this.mode)
}
