package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.casMax
import com.eignex.kumulant.stream.monotonicMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Running maximum of a stream. */
@Serializable
@SerialName("MaxResult")
data class MaxResult(
    /** Running maximum value. */
    val max: Double,
) : Result

/** Tracks the maximum of a stream. */
class MaxStat(
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<MaxResult> {

    private val mode = concurrency.monotonicMode()
    private val value = mode.newDouble(Double.NEGATIVE_INFINITY)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        casMax(this.value, value)
    }

    override fun merge(values: MaxResult) {
        casMax(value, values.max)
    }

    override fun reset() {
        value.store(Double.NEGATIVE_INFINITY)
    }

    override fun read(timestampNanos: Long) = MaxResult(value.load())

    override fun create(concurrency: Concurrency?) = MaxStat(concurrency ?: this.concurrency)
}
