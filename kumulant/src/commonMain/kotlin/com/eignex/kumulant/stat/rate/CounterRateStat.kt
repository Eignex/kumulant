package com.eignex.kumulant.stat.rate

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.additiveMode
import com.eignex.kumulant.stream.firstWriterMode
import com.eignex.kumulant.stream.serializedLock
import com.eignex.kumulant.stream.welfordMode

/**
 * Rate derived from a monotonic counter stream.
 *
 * Each update is interpreted as an absolute counter sample (for example
 * `requests_total`). The stat advances a high-water-mark counter; the running
 * total is the sum of all forward increments. The rate at read time is
 * `totalDelta / (readTs - startTs)`.
 *
 * # Concurrency
 *
 * Safe under any number of concurrent writers without manual synchronisation.
 * The update body is serialised by an internal lock; the high-water-mark
 * advances only on forward progress (`value >= previousCounter`). Decreases
 * either trigger reset accounting (when [treatDecreaseAsReset] is `true`) or
 * are silently dropped as out-of-order arrivals from another writer.
 *
 * Ordering is by **counter value**, not timestamp. Two writers submitting
 * samples with overlapping timestamps work fine as long as their counter
 * values are globally monotonic (e.g. each pulled from a shared
 * `AtomicLong`). The start timestamp converges to the earliest observed
 * predecessor via a CAS-loop, so the duration window reflects the full
 * lifetime of the stream regardless of lock-acquisition order. An explicit
 * reset (counter decrease with [treatDecreaseAsReset]) re-anchors the start
 * window to the post-reset timestamp.
 */
class CounterRateStat(
    override val concurrency: Concurrency = Concurrency.None,
    /** When `true` (default), a counter decrease is interpreted as a reset and
     *  the new sample value is counted as post-reset progress. Set to `false`
     *  to drop decreases entirely — the right choice when the underlying
     *  counter never resets and multiple writers may submit samples out of
     *  value order. */
    val treatDecreaseAsReset: Boolean = true,
) : SeriesStat<RateResult> {

    private val lock = concurrency.serializedLock()
    private val mode = concurrency.welfordMode()
    private val totalDelta = concurrency.additiveMode().newDouble(0.0)
    // CAS-capable cell so the start timestamp can converge to the earliest
    // observed predecessor across concurrent writers; striped adders
    // (HighWrite) can't CAS, so this side-steps the global mode for this one
    // cell. Cheap: only written during a forward-delta or a reset.
    private val startTimestampNanos = concurrency.firstWriterMode().newLong(Long.MIN_VALUE)
    private val lastCounter = mode.newDouble(Double.NaN)
    private val lastTimestampNanos = mode.newLong(Long.MIN_VALUE)

    override fun update(
        value: Double,
        timestampNanos: Long,
        weight: Double
    ) = lock.withLock {
        val previousCounter = lastCounter.load()
        val previousTimestamp = lastTimestampNanos.load()

        if (previousCounter.isNaN()) {
            // First observation — record state, no delta yet.
            lastCounter.store(value)
            lastTimestampNanos.store(timestampNanos)
            return@withLock
        }

        when {
            value >= previousCounter -> {
                val scaledDelta = ((value - previousCounter) * weight).coerceAtLeast(0.0)
                if (scaledDelta > 0.0) {
                    totalDelta.add(scaledDelta)
                    // Anchor (or lower) the start window to the previous sample's ts —
                    // it's the earliest observed predecessor of a real delta.
                    advanceStartTimestampDown(previousTimestamp)
                }
                lastCounter.store(value)
                lastTimestampNanos.store(timestampNanos)
            }
            treatDecreaseAsReset -> {
                // Counter restart (process restart, wrap, ...). The new value is
                // counted as post-reset progress and the start window re-anchors
                // to *this* timestamp regardless of prior state.
                val scaledDelta = (value * weight).coerceAtLeast(0.0)
                if (scaledDelta > 0.0) {
                    totalDelta.add(scaledDelta)
                    startTimestampNanos.store(timestampNanos)
                }
                lastCounter.store(value)
                lastTimestampNanos.store(timestampNanos)
            }
            // else: decrease with treatDecreaseAsReset=false — assume an
            // out-of-order arrival from a concurrent writer and drop. Leave
            // lastCounter / lastTimestampNanos alone so the next legitimate
            // forward update computes its delta against the true high-water
            // mark.
        }
    }

    private fun advanceStartTimestampDown(candidate: Long) {
        while (true) {
            val current = startTimestampNanos.load()
            if (current != Long.MIN_VALUE && current <= candidate) return
            if (startTimestampNanos.compareAndSet(current, candidate)) return
        }
    }

    override fun create(concurrency: Concurrency?) =
        CounterRateStat(concurrency ?: this.concurrency, treatDecreaseAsReset)

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
