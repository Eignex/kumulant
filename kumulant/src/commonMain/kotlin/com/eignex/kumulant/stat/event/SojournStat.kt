package com.eignex.kumulant.stat.event

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.monotonicMode
import com.eignex.kumulant.stream.serializedLock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Per-state cumulative time, transition counts, and current-state dwell. */
@Serializable
@SerialName("SojournResult")
data class SojournResult(
    /** Declared state alphabet, in the order passed to [SojournStat]. */
    val states: List<Long>,
    /** Cumulative nanoseconds spent in each declared state, in [states] order. */
    val totalNanosByState: List<Long>,
    /** Number of transitions into each declared state, in [states] order. The first
     *  update counts as an entry into its state. */
    val transitionsByState: List<Long>,
    /** State key currently occupied, or `Long.MIN_VALUE` when no observation has landed yet. */
    val currentState: Long,
    /** Timestamp (nanoseconds) at which the current state was entered. */
    val currentStateEnterTimestampNanos: Long,
    /** Timestamp (nanoseconds) at which the snapshot was taken. */
    val timestampNanos: Long,
    /** False until the first [DiscreteStat.update] has been recorded. */
    val hasState: Boolean,
) : Result {
    /** Nanoseconds spent in the current state since it was last entered. */
    val currentDwellNanos: Long
        get() = if (hasState) timestampNanos - currentStateEnterTimestampNanos else 0L
}

/**
 * Tracks total time spent in each member of a declared categorical state alphabet,
 * the number of transitions into each state, and the dwell time of the current
 * state. Update values not in [states] raise [IllegalArgumentException].
 *
 * **Use cases:** uptime / availability breakdowns by state, dwell-time
 * accounting, transition counters.
 *
 * **Memory:** O(states); three arrays of length `states.size` plus a handful of cells.
 *
 * **Update:** O(states) on transitions for the [Long]-to-index lookup
 * (linear scan over the declared alphabet); O(1) when the state does not change.
 *
 * **Concurrency:** Coupled current-state / enter-timestamp / per-state totals
 * (category 3). The internal lock keeps the multi-cell update consistent.
 */
class SojournStat(
    /** Declared state alphabet; must be non-empty and contain no duplicates. */
    val states: List<Long>,
    override val concurrency: Concurrency = Concurrency.None,
) : DiscreteStat<SojournResult> {

    init {
        require(states.isNotEmpty()) { "SojournStat requires at least one declared state" }
        require(states.distinct().size == states.size) { "SojournStat states must be unique" }
    }

    private val streamMode = concurrency.monotonicMode()
    private val lock = concurrency.serializedLock()

    private val totalNanos = streamMode.newLongArray(states.size)
    private val transitions = streamMode.newLongArray(states.size)
    private val currentStateIndex = streamMode.newLong(NO_STATE)
    private val enterTimestamp = streamMode.newLong(0L)

    private fun indexOfState(state: Long): Int {
        val idx = states.indexOf(state)
        require(idx >= 0) { "SojournStat received undeclared state $state (declared: $states)" }
        return idx
    }

    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        lock.guarded {
            val newIdx = indexOfState(value)
            val curIdx = currentStateIndex.load()
            if (curIdx == NO_STATE) {
                currentStateIndex.store(newIdx.toLong())
                enterTimestamp.store(timestampNanos)
                transitions.store(newIdx, transitions.load(newIdx) + 1L)
                return@guarded
            }
            if (curIdx.toInt() == newIdx) {
                // No transition; dwell extends silently.
                return@guarded
            }
            val priorEnter = enterTimestamp.load()
            val elapsed = (timestampNanos - priorEnter).coerceAtLeast(0L)
            totalNanos.store(curIdx.toInt(), totalNanos.load(curIdx.toInt()) + elapsed)
            currentStateIndex.store(newIdx.toLong())
            // A stamp behind the landmark is a late arrival, not a rewind: committing it would measure
            // the next transition's dwell from before this one happened, so the stat could report more
            // dwell than the stream's stamps span. The clamp above already gave this arrival no dwell.
            if (timestampNanos > priorEnter) enterTimestamp.store(timestampNanos)
            transitions.store(newIdx, transitions.load(newIdx) + 1L)
        }
    }

    override fun merge(values: SojournResult) = lock.guarded {
        require(values.states == states) { "merge state alphabet ${values.states} != $states" }
        for (i in states.indices) {
            totalNanos.store(i, totalNanos.load(i) + values.totalNanosByState[i])
            transitions.store(i, transitions.load(i) + values.transitionsByState[i])
        }
        if (!values.hasState) return@guarded
        val incomingEnter = values.currentStateEnterTimestampNanos
        val localEnter = enterTimestamp.load()
        if (currentStateIndex.load() == NO_STATE || incomingEnter > localEnter) {
            currentStateIndex.store(states.indexOf(values.currentState).toLong())
            enterTimestamp.store(incomingEnter)
        }
    }

    override fun reset() = lock.guarded {
        for (i in states.indices) {
            totalNanos.store(i, 0L)
            transitions.store(i, 0L)
        }
        currentStateIndex.store(NO_STATE)
        enterTimestamp.store(0L)
    }

    override fun read(timestampNanos: Long) = lock.guarded {
        val curIdx = currentStateIndex.load()
        val has = curIdx != NO_STATE
        SojournResult(
            states = states,
            totalNanosByState = List(states.size) { totalNanos.load(it) },
            transitionsByState = List(states.size) { transitions.load(it) },
            currentState = if (has) states[curIdx.toInt()] else Long.MIN_VALUE,
            currentStateEnterTimestampNanos = enterTimestamp.load(),
            timestampNanos = timestampNanos,
            hasState = has,
        )
    }

    override fun create(concurrency: Concurrency?) = SojournStat(states, concurrency ?: this.concurrency)

    private companion object {
        const val NO_STATE: Long = -1L
    }
}
