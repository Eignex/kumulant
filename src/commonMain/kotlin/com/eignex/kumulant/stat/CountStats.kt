package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.SerialMode
import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.core.CountResult
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.SumResult

class Count(
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<CountResult> {

    private val count = mode.newLong(0L)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        count.add(1L)
    }

    override fun merge(values: CountResult) {
        count.add(values.count)
    }

    override fun reset() {
        count.store(0L)
    }

    override fun read(timestampNanos: Long) = CountResult(count.load())

    override fun create(mode: StreamMode?) = Count(mode ?: this.mode)
}

class TotalWeights(
    val mode: StreamMode = SerialMode,
) : SeriesStat<SumResult> {

    private val totalWeights = mode.newDouble(0.0)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        totalWeights.add(weight)
    }

    override fun create(mode: StreamMode?) = TotalWeights(mode ?: this.mode)

    override fun merge(values: SumResult) {
        totalWeights.add(values.sum)
    }

    override fun reset() {
        totalWeights.store(0.0)
    }

    override fun read(timestampNanos: Long) = SumResult(totalWeights.load())
}
