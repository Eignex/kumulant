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
            // Subtract in Double, not Long: a large-magnitude negative start against a positive read
            // timestamp overflows the Long subtraction, and the guard below would read the wrapped
            // value as a non-positive duration.
            val durationSeconds = (timestampNanos.toDouble() - startTimestampNanos.toDouble()) / NANOS_PER_SECOND
            if (!(durationSeconds > 0.0)) return 0.0
            return totalValue / durationSeconds
        }
}

/**
 * Cumulative rate: total accumulated value divided by elapsed time since the
 * first update.
 *
 * For time-decaying rates that weight recent observations more heavily, see
 * [DecayingRateStat]. Use [withValue][com.eignex.kumulant.schema.ops.withValue] on
 * the [Rate][com.eignex.kumulant.schema.spec.Rate] spec to count each update as 1.
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
        // CAS-loop-min; a plain compareAndSet(MIN_VALUE, ts) would let an arbitrary
        // first-arriving thread set the start, not the thread with the earliest ts.
        var current = startTimestampNanos.load()
        while (current == Long.MIN_VALUE || current > timestampNanos) {
            if (startTimestampNanos.compareAndSet(current, timestampNanos)) break
            current = startTimestampNanos.load()
        }
        totalValues.add(value * weight)
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
