package com.eignex.kumulant.stat.rate

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isNotPositiveWeight
import com.eignex.kumulant.stream.additiveMode
import com.eignex.kumulant.stream.firstWriterMode
import com.eignex.kumulant.stream.guarded
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
 * Ordering is by **counter value**, not timestamp. Two writers submitting
 * samples with overlapping timestamps work fine as long as their counter
 * values are globally monotonic (e.g. each pulled from a shared `AtomicLong`).
 * An explicit reset (counter decrease with [treatDecreaseAsReset]) re-anchors
 * the start window to the post-reset timestamp.
 *
 * **Use cases:** scraping monotonic counters (Prometheus `*_total` metrics,
 * OS counter readings, replicated request counts). Pair with a Prometheus-style
 * scraper that periodically samples an external counter.
 *
 * **Memory:** O(1); total delta + start timestamp + `(lastCounter, lastTs)`.
 *
 * **Update:** O(1) per observation under the per-stat lock.
 *
 * **Concurrency:** Body locked under any concurrent [Concurrency] level
 * (no-op under [Concurrency.None]); safe under any number of concurrent
 * writers without external synchronisation. Forward-progressing samples
 * advance the high-water mark; decreases either reset (when
 * [treatDecreaseAsReset]) or drop silently. The start timestamp converges to
 * the earliest observed predecessor via CAS-loop-min.
 */
class CounterRateStat(
    override val concurrency: Concurrency = Concurrency.None,
    /** When `true` (default), a counter decrease is interpreted as a reset and
     *  the new sample value is counted as post-reset progress. Set to `false`
     *  to drop decreases entirely; the right choice when the underlying
     *  counter never resets and multiple writers may submit samples out of
     *  value order. */
    val treatDecreaseAsReset: Boolean = true,
) : SeriesStat<RateResult> {

    private val lock = concurrency.serializedLock()
    private val mode = concurrency.welfordMode()
    private val totalDelta = concurrency.additiveMode().newDouble(0.0)
    private val startTimestampNanos = concurrency.firstWriterMode().newLong(Long.MIN_VALUE)
    private val lastCounter = mode.newDouble(Double.NaN)
    private val lastTimestampNanos = mode.newLong(Long.MIN_VALUE)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        // Return before touching lastCounter: a weight that contributes no delta would still advance
        // the high-water mark, destroying the increment for good so no later sample could recover it.
        // A negative weight is dropped rather than downdated for the same reason - the mark has no
        // inverse, and the clamp below would swallow the delta while the mark moved on anyway. NaN is
        // dropped as it is everywhere; see Stat. Outside the lock, like every other stat's inert
        // guard - a no-op has no state to protect.
        if (weight.isNotPositiveWeight()) return
        lock.guarded { updateLocked(value, timestampNanos, weight) }
    }

    private fun updateLocked(value: Double, timestampNanos: Long, weight: Double) {
        val previousCounter = lastCounter.load()
        val previousTimestamp = lastTimestampNanos.load()

        if (previousCounter.isNaN()) {
            lastCounter.store(value)
            lastTimestampNanos.store(timestampNanos)
            return
        }

        when {
            value >= previousCounter -> {
                val scaledDelta = ((value - previousCounter) * weight).coerceAtLeast(0.0)
                if (scaledDelta > 0.0) {
                    totalDelta.add(scaledDelta)
                    advanceStartTimestampDown(previousTimestamp)
                }
                lastCounter.store(value)
                lastTimestampNanos.store(timestampNanos)
            }

            treatDecreaseAsReset -> {
                // Registered for any decrease, including a restart to exactly 0.0 - the most common
                // one a *_total counter makes. Gating on post-reset progress would conflate "the
                // reset has produced nothing yet" with "no reset happened", keeping every pre-reset
                // increment and the old anchor while the high-water mark moved on regardless.
                //
                // Re-anchoring the window without clearing the total would divide every pre-reset
                // increment by the short post-reset duration: a counter advancing 105 units over 11s
                // would report 210/s. The window and the total describe the same span, so they have
                // to be reset together.
                totalDelta.store((value * weight).coerceAtLeast(0.0))
                startTimestampNanos.store(timestampNanos)
                lastCounter.store(value)
                lastTimestampNanos.store(timestampNanos)
            }
            // Decrease with treatDecreaseAsReset=false: drop without touching
            // lastCounter/lastTimestampNanos so the next forward update computes its
            // delta against the true high-water mark.
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

    override fun read(timestampNanos: Long): RateResult = lock.guarded {
        val start = if (startTimestampNanos.load() == Long.MIN_VALUE) {
            timestampNanos
        } else {
            startTimestampNanos.load()
        }
        RateResult(
            startTimestampNanos = start,
            totalValue = totalDelta.load(),
            timestampNanos = timestampNanos,
        )
    }

    override fun merge(values: RateResult, workspace: com.eignex.koblas.Workspace?) = lock.guarded {
        totalDelta.add(values.totalValue)

        // See RateStat.merge: an untouched shard reports its read timestamp as the start and so has no
        // window to adopt, but a zero total does not imply an untouched shard.
        if (values.startTimestampNanos >= values.timestampNanos) return@guarded

        val currentStart = startTimestampNanos.load()
        if (currentStart == Long.MIN_VALUE || values.startTimestampNanos < currentStart) {
            startTimestampNanos.store(values.startTimestampNanos)
        }
    }

    override fun reset() = lock.guarded {
        totalDelta.store(0.0)
        lastCounter.store(Double.NaN)
        lastTimestampNanos.store(Long.MIN_VALUE)
        startTimestampNanos.store(Long.MIN_VALUE)
    }
}
