package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.welfordLock
import com.eignex.kumulant.stream.welfordMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Arithmetic mean. */
@Serializable
@SerialName("MeanStat")
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
 * Numerically stable across wide dynamic ranges; merges two [MeanStat]s using Chan's
 * parallel algorithm.
 */
class MeanStat(
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<WeightedMeanResult> {

    private val mode = concurrency.welfordMode()
    private val lock = concurrency.welfordLock()
    private val totalWeights = mode.newDouble(0.0)
    private val mean = mode.newDouble(0.0)

    override fun update(value: Double, timestampNanos: Long, weight: Double) = lock.withLock {
        if (weight == 0.0) return@withLock
        val nextW = totalWeights.addAndGet(weight)
        val delta = value - mean.load()
        mean.add(delta * (weight / nextW))
    }

    override fun read(timestampNanos: Long): WeightedMeanResult = lock.withLock {
        WeightedMeanResult(totalWeights.load(), mean.load())
    }

    override fun merge(values: WeightedMeanResult) = lock.withLock {
        if (values.totalWeights <= 0.0) return@withLock
        val nextW = totalWeights.addAndGet(values.totalWeights)
        val delta = values.mean - mean.load()
        mean.add(delta * (values.totalWeights / nextW))
    }

    override fun reset() = lock.withLock {
        totalWeights.store(0.0)
        mean.store(0.0)
    }

    override fun create(concurrency: Concurrency?) = MeanStat(concurrency ?: this.concurrency)
}
