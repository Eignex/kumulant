package com.eignex.kumulant.stat.event

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.stream.additiveMode
import com.eignex.kumulant.stream.casMax
import com.eignex.kumulant.stream.monotonicMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Counts of upward and downward crossings of a fixed level. */
@Serializable
@SerialName("CrossingResult")
data class CrossingResult(
    /** Level the input stream was compared against. */
    val level: Double,
    /** Number of strict transitions from `value < level` to `value >= level`. */
    val upCrossings: Long,
    /** Number of strict transitions from `value >= level` to `value < level`. */
    val downCrossings: Long,
    /**
     * Whether the last observed value was at or above [level], or `null` before any observation.
     *
     * Carried so a merge into a stat with no baseline of its own can continue from the side the
     * snapshot left off on, instead of spending its next update establishing one afresh and losing
     * the crossing that update represents.
     */
    val endedAtOrAboveLevel: Boolean? = null,
) : Result

/**
 * Counts strict upward and downward crossings of a configured [level].
 *
 * The very first update establishes the baseline side and is not counted as a
 * crossing. Subsequent updates increment [CrossingResult.upCrossings] when the
 * value transitions from below to at-or-above [level], and
 * [CrossingResult.downCrossings] on the reverse.
 *
 * **Use cases:** alarm/recovery counting, level-crossing diagnostics, frequency
 * estimation of band-limited signals.
 *
 * **Memory:** O(1); three cells plus a baseline flag.
 *
 * **Update:** O(1).
 *
 * **Concurrency:** Per-cell atomics with bounded drift (category 1).
 * Concurrent updates may briefly observe the previous side as stale and either
 * over- or under-count by a small amount; the counts themselves are monotonic.
 */
class CrossingStat(
    /** The level to compare against. */
    val level: Double,
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<CrossingResult> {

    private val mode = concurrency.monotonicMode()
    private val additive = concurrency.additiveMode()
    private val initialized = mode.newLong(0L)
    private val lastSide = mode.newLong(0L)
    private val ups = additive.newLong(0L)
    private val downs = additive.newLong(0L)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        val side = if (value >= level) 1L else 0L
        if (initialized.addAndGet(1L) == 1L) {
            lastSide.store(side)
            return
        }
        val prev = lastSide.load()
        if (prev != side) {
            if (side == 1L) ups.add(1L) else downs.add(1L)
            lastSide.store(side)
        }
    }

    override fun merge(values: CrossingResult) {
        // The counts only mean anything against the level they were measured at: folding a snapshot
        // taken at another level in would report crossings of a different predicate as this one's.
        require(values.level == level) { "merge level ${values.level} != $level" }
        ups.add(values.upCrossings)
        downs.add(values.downCrossings)
        // Adopt the baseline only when there is none, the way SojournStat keeps its own current state
        // over an incoming one: with a baseline already established, the local stream's side is the
        // live one and the snapshot's is a parallel shard's, which nothing here can order against it.
        val endedAbove = values.endedAtOrAboveLevel ?: return
        if (initialized.load() == 0L) {
            lastSide.store(if (endedAbove) 1L else 0L)
            casMax(initialized, 1L)
        }
    }

    override fun reset() {
        initialized.store(0L)
        lastSide.store(0L)
        ups.store(0L)
        downs.store(0L)
    }

    override fun read(timestampNanos: Long) = CrossingResult(
        level = level,
        upCrossings = ups.load(),
        downCrossings = downs.load(),
        endedAtOrAboveLevel = if (initialized.load() == 0L) null else lastSide.load() == 1L,
    )

    override fun create(concurrency: Concurrency?) = CrossingStat(level, concurrency ?: this.concurrency)
}
