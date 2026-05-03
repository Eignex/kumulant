package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.StreamMode
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
 * parallel algorithm.
 */
class Mean(
    override val mode: StreamMode = defaultStreamMode,
) : SeriesStat<WeightedMeanResult> {

    private val totalWeights = mode.newDouble(0.0)
    private val mean = mode.newDouble(0.0)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight == 0.0) return
        val nextW = totalWeights.addAndGet(weight)
        val delta = value - mean.load()
        mean.add(delta * (weight / nextW))
    }

    override fun read(timestampNanos: Long): WeightedMeanResult =
        WeightedMeanResult(totalWeights.load(), mean.load())

    override fun merge(values: WeightedMeanResult) {
        if (values.totalWeights <= 0.0) return
        val nextW = totalWeights.addAndGet(values.totalWeights)
        val delta = values.mean - mean.load()
        mean.add(delta * (values.totalWeights / nextW))
    }

    override fun reset() {
        totalWeights.store(0.0)
        mean.store(0.0)
    }

    override fun create(mode: StreamMode?) = Mean(mode ?: this.mode)
}
