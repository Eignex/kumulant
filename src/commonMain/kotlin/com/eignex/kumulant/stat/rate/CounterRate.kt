package com.eignex.kumulant.stat.rate

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.welfordLock
import com.eignex.kumulant.stream.welfordMode

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
    override val concurrency: Concurrency = Concurrency.None,
    val treatDecreaseAsReset: Boolean = true,
) : SeriesStat<RateResult> {

    private val mode = concurrency.welfordMode()
    private val lock = concurrency.welfordLock()
    private val totalDelta = mode.newDouble(0.0)
    private val startTimestampNanos = mode.newLong(Long.MIN_VALUE)
    private val lastCounter = mode.newDouble(Double.NaN)
    private val lastTimestampNanos = mode.newLong(Long.MIN_VALUE)

    override fun update(
        value: Double,
        timestampNanos: Long,
        weight: Double
    ) = lock.withLock {
        val previousCounter = lastCounter.load()
        val previousTimestamp = lastTimestampNanos.load()

        if (previousTimestamp == Long.MIN_VALUE || previousCounter.isNaN()) {
            lastCounter.store(value)
            lastTimestampNanos.store(timestampNanos)
            return@withLock
        }

        if (timestampNanos <= previousTimestamp) return@withLock

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

    override fun create(concurrency: Concurrency?) =
        CounterRate(concurrency ?: this.concurrency, treatDecreaseAsReset)

    override fun read(timestampNanos: Long): RateResult = lock.withLock {
        val start = if (startTimestampNanos.load() == Long.MIN_VALUE) {
            timestampNanos
        } else {
            startTimestampNanos.load()
        }
        RateResult(
            startTimestampNanos = start,
            totalValue = totalDelta.load(),
            timestampNanos = timestampNanos
        )
    }

    override fun merge(values: RateResult) = lock.withLock {
        if (values.totalValue == 0.0) return@withLock

        totalDelta.add(values.totalValue)

        val currentStart = startTimestampNanos.load()
        if (currentStart == Long.MIN_VALUE || values.startTimestampNanos < currentStart) {
            startTimestampNanos.store(values.startTimestampNanos)
        }
    }

    override fun reset() = lock.withLock {
        totalDelta.store(0.0)
        lastCounter.store(Double.NaN)
        lastTimestampNanos.store(Long.MIN_VALUE)
        startTimestampNanos.store(Long.MIN_VALUE)
    }
}
