package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.core.RateResult
import com.eignex.kumulant.core.SeriesStat

/**
 * Cumulative rate: total accumulated value divided by elapsed time since the
 * first update. Useful for measuring throughput over the lifetime of a stream.
 *
 * For time-decaying rates that weight recent observations more heavily,
 * see [DecayingRate]. Use [withValue][com.eignex.kumulant.operation.withValue] to count each update as 1.
 */
class Rate(
    val mode: StreamMode = defaultStreamMode,
) : SeriesStat<RateResult> {

    private val totalValues = mode.newDouble(0.0)
    private val startTimestampNanos = mode.newLong(Long.MIN_VALUE)

    override fun update(
        value: Double,
        timestampNanos: Long,
        weight: Double
    ) {
        if (startTimestampNanos.load() == Long.MIN_VALUE) {
            startTimestampNanos.store(timestampNanos)
        }
        totalValues.add(value * weight)
    }

    override fun create(mode: StreamMode?) = Rate(mode ?: this.mode)

    override fun read(timestampNanos: Long): RateResult {
        val start = if (startTimestampNanos.load() == Long.MIN_VALUE) {
            timestampNanos
        } else {
            startTimestampNanos.load()
        }
        return RateResult(
            startTimestampNanos = start,
            totalValue = totalValues.load(),
            timestampNanos = timestampNanos
        )
    }

    override fun merge(values: RateResult) {
        if (values.totalValue == 0.0) return

        totalValues.add(values.totalValue)

        val currentStart = startTimestampNanos.load()
        if (currentStart == Long.MIN_VALUE || values.startTimestampNanos < currentStart) {
            startTimestampNanos.store(values.startTimestampNanos)
        }
    }

    override fun reset() {
        startTimestampNanos.store(Long.MIN_VALUE)
        totalValues.store(0.0)
    }
}

/**
 * Rate derived from a monotonic counter stream.
 *
 * Each update is interpreted as an absolute counter sample (for example
 * requests_total). The stat accumulates positive deltas between successive
 * samples and reports them as [RateResult].
 *
 * By default, a counter decrease is treated as a reset and the new sample
 * value is counted as post-reset progress.
 */
class CounterRate(
    val mode: StreamMode = defaultStreamMode,
    val treatDecreaseAsReset: Boolean = true,
) : SeriesStat<RateResult> {

    private val totalDelta = mode.newDouble(0.0)
    private val startTimestampNanos = mode.newLong(Long.MIN_VALUE)
    private val lastCounter = mode.newDouble(Double.NaN)
    private val lastTimestampNanos = mode.newLong(Long.MIN_VALUE)

    override fun update(
        value: Double,
        timestampNanos: Long,
        weight: Double
    ) {
        val previousCounter = lastCounter.load()
        val previousTimestamp = lastTimestampNanos.load()

        if (previousTimestamp == Long.MIN_VALUE || previousCounter.isNaN()) {
            lastCounter.store(value)
            lastTimestampNanos.store(timestampNanos)
            return
        }

        if (timestampNanos <= previousTimestamp) return

        val isReset = value < previousCounter
        val rawDelta = when {
            !isReset -> value - previousCounter
            treatDecreaseAsReset -> value
            else -> 0.0
        }
        val scaledDelta = (rawDelta * weight).coerceAtLeast(0.0)

        if (scaledDelta > 0.0) {
            if (startTimestampNanos.load() == Long.MIN_VALUE) {
                startTimestampNanos.store(
                    if (isReset) timestampNanos else previousTimestamp
                )
            }
            totalDelta.add(scaledDelta)
        }

        lastCounter.store(value)
        lastTimestampNanos.store(timestampNanos)
    }

    override fun create(mode: StreamMode?) =
        CounterRate(mode ?: this.mode, treatDecreaseAsReset)

    override fun read(timestampNanos: Long): RateResult {
        val start = if (startTimestampNanos.load() == Long.MIN_VALUE) {
            timestampNanos
        } else {
            startTimestampNanos.load()
        }
        return RateResult(
            startTimestampNanos = start,
            totalValue = totalDelta.load(),
            timestampNanos = timestampNanos
        )
    }

    override fun merge(values: RateResult) {
        if (values.totalValue == 0.0) return

        totalDelta.add(values.totalValue)

        val currentStart = startTimestampNanos.load()
        if (currentStart == Long.MIN_VALUE || values.startTimestampNanos < currentStart) {
            startTimestampNanos.store(values.startTimestampNanos)
        }
    }

    override fun reset() {
        totalDelta.store(0.0)
        startTimestampNanos.store(Long.MIN_VALUE)
        lastCounter.store(Double.NaN)
        lastTimestampNanos.store(Long.MIN_VALUE)
    }
}
