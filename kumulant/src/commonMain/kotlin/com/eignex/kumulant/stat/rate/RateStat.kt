package com.eignex.kumulant.stat.rate

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasRate
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.stream.NANOS_PER_SECOND
import com.eignex.kumulant.stream.additiveMode
import com.eignex.kumulant.stream.firstWriterMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Cumulative rate: [totalValue] accumulated from [startTimestampNanos] to [timestampNanos]. */
@Serializable
@SerialName("RateResult")
data class RateResult(val startTimestampNanos: Long, val totalValue: Double, val timestampNanos: Long) :
    Result,
    HasRate {
    override val rate: Double
        get() {
            if (timestampNanos <= startTimestampNanos) return 0.0
            // Subtract before converting to preserve short intervals at large timestamps. A negative
            // difference here can only be Long overflow, for which Double subtraction is safe.
            val elapsed = timestampNanos - startTimestampNanos
            val durationNanos = if (elapsed > 0L) {
                elapsed.toDouble()
            } else {
                timestampNanos.toDouble() - startTimestampNanos.toDouble()
            }
            val durationSeconds = durationNanos / NANOS_PER_SECOND
            return totalValue / durationSeconds
        }
}

/**
 * Cumulative rate: total accumulated value divided by elapsed time since the
 * first update.
 *
 * For time-decaying rates that weight recent observations more heavily, read
 * [DecayingSumResult.rate][com.eignex.kumulant.stat.decay.DecayingSumResult.rate].
 * Use [withValue][com.eignex.kumulant.schema.ops.withValue] on the
 * [Rate][com.eignex.kumulant.schema.spec.Rate] spec to count each update as 1.
 *
 * **Use cases:** lifetime throughput (requests/sec since start, total bytes
 * divided by uptime). Pair with `withValue(1.0)` for an event-rate counter.
 *
 * **Memory:** O(1); total value cell + start timestamp.
 *
 * **Update:** O(1) per observation; one atomic add plus a CAS-loop-min on the
 * start timestamp (loop terminates immediately on the common path).
 *
 * **Concurrency:** Single atomic add for the total + CAS-loop-min for the
 * start timestamp. Lock-free and exact under every [Concurrency] level.
 * [Concurrency.HighWrite] switches the total cell to a striped adder; the
 * start timestamp stays an `AtomicLong` (the striped adder doesn't support
 * CAS).
 */
class RateStat(override val concurrency: Concurrency = Concurrency.None) : SeriesStat<RateResult> {

    private val totalValues = concurrency.additiveMode().newDouble(0.0)
    private val startTimestampNanos = concurrency.firstWriterMode().newLong(Long.MIN_VALUE)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        // Return before the CAS loop, not just before the accumulator: a zero-weight observation must
        // not anchor the rate window either, or the denominator would count time the numerator never
        // saw. See Stat.
        if (weight.isInertWeight()) return
        advanceStartTimestampDown(timestampNanos)
        totalValues.add(value * weight)
    }

    private fun advanceStartTimestampDown(timestampNanos: Long) {
        // Updates and merges share the CAS loop so an interleaved later stamp cannot overwrite
        // an earlier one elected by another writer.
        var current = startTimestampNanos.load()
        while (current == Long.MIN_VALUE || current > timestampNanos) {
            if (startTimestampNanos.compareAndSet(current, timestampNanos)) break
            current = startTimestampNanos.load()
        }
    }

    override fun create(concurrency: Concurrency?) = RateStat(concurrency ?: this.concurrency)

    override fun read(timestampNanos: Long): RateResult {
        val start = if (startTimestampNanos.load() == Long.MIN_VALUE) {
            timestampNanos
        } else {
            startTimestampNanos.load()
        }
        return RateResult(
            startTimestampNanos = start,
            totalValue = totalValues.load(),
            timestampNanos = timestampNanos,
        )
    }

    override fun merge(values: RateResult, timestampNanos: Long) {
        totalValues.add(values.totalValue)

        // An untouched shard has zero total and reports its read timestamp as the start. A nonzero
        // total proves the window exists even if the snapshot was read at or before its start.
        // Zero-valued observations over a positive span also contribute their window.
        if (values.totalValue == 0.0 && values.startTimestampNanos >= values.timestampNanos) return

        advanceStartTimestampDown(values.startTimestampNanos)
    }

    override fun reset() {
        startTimestampNanos.store(Long.MIN_VALUE)
        totalValues.store(0.0)
    }
}
