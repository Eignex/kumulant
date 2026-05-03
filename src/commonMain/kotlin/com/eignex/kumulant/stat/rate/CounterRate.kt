package com.eignex.kumulant.stat.rate

import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.defaultStreamMode

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
    override val mode: StreamMode = defaultStreamMode,
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
        lastCounter.store(Double.NaN)
        lastTimestampNanos.store(Long.MIN_VALUE)
        startTimestampNanos.store(Long.MIN_VALUE)
    }
}
