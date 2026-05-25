package com.eignex.kumulant.stat.event

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.casMax
import com.eignex.kumulant.stream.monotonicMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Time since the most recent observation; [hasObservation] is false until the first update. */
@Serializable
@SerialName("RecencyResult")
data class RecencyResult(
    /** Timestamp (nanoseconds) of the most recent observation, or [Long.MIN_VALUE] before any update. */
    val lastObservedTimestampNanos: Long,
    /** Timestamp (nanoseconds) at which the snapshot was taken. */
    val timestampNanos: Long,
    /** False until the first [SeriesStat.update] has been recorded. */
    val hasObservation: Boolean,
) : Result {
    /**
     * Nanoseconds elapsed between the last observation and [timestampNanos]; `-1L` when
     * no observation has been recorded.
     */
    val elapsedNanos: Long get() = if (hasObservation) timestampNanos - lastObservedTimestampNanos else -1L
}

/**
 * Reports the time elapsed since the most recent observation. Compose with `.filter(...)`
 * to track recency of observations matching a predicate (for example: "how long since the
 * last error").
 *
 * **Use cases:** liveness checks, "last seen" diagnostics, staleness detection.
 *
 * **Memory:** O(1) — two cells.
 *
 * **Update:** O(1) — single CAS on a monotonic timestamp cell.
 *
 * **Concurrency:** Per-cell atomics with bounded drift (category 1). Concurrent updates
 * race on [casMax]; the latest timestamp wins.
 */
class RecencyStat(override val concurrency: Concurrency = Concurrency.None) : SeriesStat<RecencyResult> {

    private val streamMode = concurrency.monotonicMode()
    private val lastObserved = streamMode.newLong(Long.MIN_VALUE)
    private val seen = streamMode.newLong(0L)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        casMax(lastObserved, timestampNanos)
        seen.store(1L)
    }

    override fun merge(values: RecencyResult) {
        if (values.hasObservation) {
            casMax(lastObserved, values.lastObservedTimestampNanos)
            seen.store(1L)
        }
    }

    override fun reset() {
        lastObserved.store(Long.MIN_VALUE)
        seen.store(0L)
    }

    override fun read(timestampNanos: Long) = RecencyResult(
        lastObservedTimestampNanos = lastObserved.load(),
        timestampNanos = timestampNanos,
        hasObservation = seen.load() != 0L,
    )

    override fun create(concurrency: Concurrency?) = RecencyStat(concurrency ?: this.concurrency)
}
