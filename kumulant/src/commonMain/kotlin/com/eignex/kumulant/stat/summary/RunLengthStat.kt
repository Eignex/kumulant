package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.casMax
import com.eignex.kumulant.stream.monotonicMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Current and longest consecutive truthy-run lengths. */
@Serializable
@SerialName("RunLengthResult")
data class RunLengthResult(
    /** Length of the current run; reset to zero when the input was last falsy. */
    val current: Long,
    /** Longest run observed so far. */
    val longest: Long,
) : Result

/**
 * Tracks the length of the current consecutive truthy run and the longest
 * observed run. A value is treated as truthy when it is non-zero and not NaN;
 * compose with `.transform(...)` upstream to project arbitrary predicates onto
 * `0.0` / `1.0` before they reach the stat.
 *
 * **Use cases:** streak length, "consecutive failed checks", run-length encoding.
 *
 * **Memory:** O(1) — two cells.
 *
 * **Update:** O(1).
 *
 * **Concurrency:** Per-cell atomics with bounded drift (category 1). Concurrent
 * updates can interleave so the current-run cell briefly diverges from a serial
 * replay; the longest-run cell is monotonic via [casMax].
 */
class RunLengthStat(override val concurrency: Concurrency = Concurrency.None) : SeriesStat<RunLengthResult> {

    private val mode = concurrency.monotonicMode()
    private val current = mode.newLong(0L)
    private val longest = mode.newLong(0L)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (value != 0.0 && !value.isNaN()) {
            val updated = current.addAndGet(1L)
            casMax(longest, updated)
        } else {
            current.store(0L)
        }
    }

    override fun merge(values: RunLengthResult) {
        casMax(longest, values.longest)
        casMax(longest, current.load() + values.current)
        current.add(values.current)
    }

    override fun reset() {
        current.store(0L)
        longest.store(0L)
    }

    override fun read(timestampNanos: Long) = RunLengthResult(current.load(), longest.load())

    override fun create(concurrency: Concurrency?) = RunLengthStat(concurrency ?: this.concurrency)
}
