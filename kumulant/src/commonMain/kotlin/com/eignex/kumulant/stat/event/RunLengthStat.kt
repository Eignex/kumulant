package com.eignex.kumulant.stat.event

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isInertWeight
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
    /**
     * Length of the run this stream *opened* with, before its first falsy value.
     *
     * Only [RunLengthStat.merge] needs it, and it is what makes that merge correct: concatenating
     * two streams can create a run spanning the join, whose length is the left stream's *trailing*
     * run plus the right stream's *leading* one.
     */
    val leading: Long = current,
    /**
     * False while the stream has been one unbroken run, i.e. [leading] covers all of it.
     *
     * Needed because concatenation only extends the leading and trailing runs through a stream that
     * never broke; otherwise they are pinned by the falsy value that broke it.
     */
    val anyFalsy: Boolean = false,
) : Result

/**
 * Tracks the length of the current consecutive truthy run and the longest
 * observed run. A value is treated as truthy when it is non-zero and not NaN;
 * compose with `.transform(...)` upstream to project arbitrary predicates onto
 * `0.0` / `1.0` before they reach the stat.
 *
 * **Use cases:** streak length, "consecutive failed checks", run-length encoding.
 *
 * **Memory:** O(1); two cells.
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
    private val leading = mode.newLong(0L)
    private val anyFalsy = mode.newLong(0L)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        // This stat's input is a predicate projected onto 0.0 / 1.0 rather than a measurement, so a
        // NaN reads as "not satisfied" and breaks the run. That is propagation, not filtering: a run
        // length stays a well-defined integer whatever arrives, so there is no NaN to carry forward.
        if (value != 0.0 && !value.isNaN()) {
            val updated = current.addAndGet(1L)
            casMax(longest, updated)
            // Still one unbroken run, so the opening run is everything so far.
            if (anyFalsy.load() == 0L) leading.store(updated)
        } else {
            current.store(0L)
            anyFalsy.store(1L)
        }
    }

    override fun merge(values: RunLengthResult, workspace: com.eignex.koblas.Workspace?) {
        val leftCurrent = current.load()
        val leftLeading = leading.load()
        val leftBroken = anyFalsy.load() != 0L

        casMax(longest, values.longest)
        // The run spanning the join: this stream's trailing run plus the incoming stream's leading
        // one. This is the only place the two can combine into something longer than either.
        casMax(longest, leftCurrent + values.leading)

        // The trailing run carries through the incoming stream only if it never broke.
        current.store(if (values.anyFalsy) values.current else leftCurrent + values.current)
        // Symmetrically, the leading run extends into the incoming stream only if *this* one never
        // broke.
        if (!leftBroken) leading.store(leftLeading + values.leading)
        if (leftBroken || values.anyFalsy) anyFalsy.store(1L)
    }

    override fun reset() {
        current.store(0L)
        longest.store(0L)
        leading.store(0L)
        anyFalsy.store(0L)
    }

    override fun read(timestampNanos: Long) = RunLengthResult(
        current = current.load(),
        longest = longest.load(),
        leading = leading.load(),
        anyFalsy = anyFalsy.load() != 0L,
    )

    override fun create(concurrency: Concurrency?) = RunLengthStat(concurrency ?: this.concurrency)
}
