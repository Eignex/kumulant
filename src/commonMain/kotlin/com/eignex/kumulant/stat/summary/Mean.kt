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
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<WeightedMeanResult> {

    private val totalWeights = mode.newDouble(0.0)
    private val value = mode.newDouble(0.0)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        val oldMean = this.value.load()
        val nextW = totalWeights.addAndGet(weight)

        val delta = value - oldMean
        val r = delta * (weight / nextW)

        this.value.add(r)
    }

    override fun read(timestampNanos: Long) =
        WeightedMeanResult(totalWeights.load(), value.load())

    override fun merge(values: WeightedMeanResult) {
        if (values.totalWeights <= 0.0) return

        val nextW = totalWeights.load() + values.totalWeights
        val delta = values.mean - value.load()
        val deltaM = delta * (values.totalWeights / nextW)

        value.add(deltaM)
        totalWeights.add(values.totalWeights)
    }

    override fun reset() {
        totalWeights.store(0.0)
        value.store(0.0)
    }

    override fun create(mode: StreamMode?) = Mean(mode ?: this.mode)
}
