package com.eignex.katom.stat

import com.eignex.katom.concurrent.SerialMode
import com.eignex.katom.concurrent.StreamMode
import com.eignex.katom.concurrent.defaultStreamMode
import com.eignex.katom.concurrent.getValue
import com.eignex.katom.core.*
import kotlin.time.Duration

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

    override fun copy(
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

    override fun copy(
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

class EventRate(
    val mode: StreamMode = defaultStreamMode,
    override val name: String? = null
) : SeriesStat<RateResult>, HasRate {

    private val _rate = Rate(mode, name)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        _rate.update(1.0, timestampNanos)
    }

    override fun copy(
        mode: StreamMode?,
        name: String?
    ) = EventRate(mode ?: this.mode, name ?: this.name)

    override fun merge(values: RateResult) {
        _rate.merge(values)
    }

    override fun reset() {
        _rate.reset()
    }

    override fun read(timestampNanos: Long) = _rate.read(timestampNanos)

    override val rate: Double get() = _rate.rate
}

class DecayingEventRate(
    val halfLife: Duration,
    val mode: StreamMode = defaultStreamMode,
    override val name: String? = null
) : SeriesStat<DecayingRateResult>, HasRate {
    private val _rate = DecayingRate(halfLife, mode, name)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        _rate.update(1.0, timestampNanos)
    }

    override fun copy(
        mode: StreamMode?,
        name: String?
    ) = DecayingEventRate(halfLife, mode ?: this.mode, name ?: this.name)

    override fun merge(values: DecayingRateResult) {
        _rate.merge(values)
    }

    override fun reset() {
        _rate.reset()
    }

    override fun read(timestampNanos: Long) = _rate.read(timestampNanos)
    override val rate: Double get() = _rate.rate
}
