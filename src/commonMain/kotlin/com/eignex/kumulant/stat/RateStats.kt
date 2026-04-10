package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.core.HasRate
import com.eignex.kumulant.core.RateResult
import com.eignex.kumulant.core.SeriesStat

/**
 * Cumulative rate: total accumulated value divided by elapsed time since the
 * first update. Useful for measuring throughput over the lifetime of a stream.
 *
 * For time-decaying rates that weight recent observations more heavily,
 * see [DecayingRate]. Use [withValue][com.eignex.kumulant.core.withValue] to count each update as 1.
 */
class Rate(
    val mode: StreamMode = defaultStreamMode,
    override val name: String? = null
) : SeriesStat<RateResult>, HasRate {

    private val _totalValues = mode.newDouble(0.0)
    private val _startTimestampNanos = mode.newLong(Long.MIN_VALUE)
    val startTimestampNanos: Long get() = _startTimestampNanos.load()

    override fun update(
        value: Double,
        timestampNanos: Long,
        weight: Double
    ) {
        if (_startTimestampNanos.load() == Long.MIN_VALUE) {
            _startTimestampNanos.store(timestampNanos)
        }
        _totalValues.add(value * weight)
    }

    override fun create(
        mode: StreamMode?,
        name: String?
    ) = Rate(mode ?: this.mode, name ?: this.name)

    override fun read(timestampNanos: Long): RateResult {
        val start = if (_startTimestampNanos.load() == Long.MIN_VALUE) {
            timestampNanos
        } else {
            _startTimestampNanos.load()
        }
        return RateResult(
            startTimestampNanos = start,
            totalValue = _totalValues.load(),
            timestampNanos = timestampNanos,
            name = name
        )
    }

    override fun merge(values: RateResult) {
        if (values.totalValue == 0.0) return

        _totalValues.add(values.totalValue)

        val currentStart = _startTimestampNanos.load()
        if (currentStart == Long.MIN_VALUE || values.startTimestampNanos < currentStart) {
            _startTimestampNanos.store(values.startTimestampNanos)
        }
    }

    override fun reset() {
        _startTimestampNanos.store(Long.MIN_VALUE)
        _totalValues.store(0.0)
    }

    override val rate: Double get() = read().rate
}

