package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.SerialMode
import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.concurrent.getValue
import com.eignex.kumulant.core.CountResult
import com.eignex.kumulant.core.HasCount
import com.eignex.kumulant.core.HasTotalWeights
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.SumResult

class Count(
    val mode: StreamMode = defaultStreamMode,
    override val name: String? = null
) : SeriesStat<CountResult>, HasCount {

    private val _count = mode.newLong(0L)
    override val count: Long by _count

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        _count.add(1L)
    }

    override fun merge(values: CountResult) {
        _count.add(values.count)
    }

    override fun reset() {
        _count.store(0L)
    }

    override fun read(timestampNanos: Long) = CountResult(count, name)

    override fun create(
        mode: StreamMode?,
        name: String?
    ) = Count(mode ?: this.mode, name ?: this.name)
}

class TotalWeights(
    val mode: StreamMode = SerialMode,
    override val name: String? = null
) : SeriesStat<SumResult>, HasTotalWeights {

    private val _totalWeights = mode.newDouble(0.0)
    override val totalWeights: Double by _totalWeights

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        _totalWeights.add(weight)
    }

    override fun create(
        mode: StreamMode?,
        name: String?
    ) = TotalWeights(mode ?: this.mode, name ?: this.name)

    override fun merge(values: SumResult) {
        _totalWeights.add(values.sum)
    }

    override fun reset() {
        _totalWeights.store(0.0)
    }

    override fun read(timestampNanos: Long) = SumResult(totalWeights, name)
}

