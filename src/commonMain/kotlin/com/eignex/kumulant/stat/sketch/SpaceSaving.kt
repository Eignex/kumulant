package com.eignex.kumulant.stat.sketch

import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.defaultStreamMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Space-Saving heavy-hitters snapshot. [keys], [counts], [errors] are parallel arrays of
 * length ≤ [capacity]; for each tracked key, [counts] is the (over)estimated weighted
 * count and [errors] is the Space-Saving overestimate bound (the count is at most this
 * much above the true count). [totalSeen] is the unweighted update count.
 */
@Serializable
@SerialName("HeavyHitters")
data class HeavyHittersResult(
    val capacity: Int,
    val keys: LongArray,
    val counts: LongArray,
    val errors: LongArray,
    val totalSeen: Long,
) : Result

/**
 * Space-Saving heavy-hitters tracker (Metwally, Agrawal, El Abbadi 2005). Maintains up
 * to [capacity] (key, count, error) triples; on a miss when full, the minimum-count slot
 * is evicted and the new key inherits the old count plus its weight, with the old count
 * recorded as the new key's overestimate bound.
 *
 * Reported counts are one-sided overestimates: `count >= true count` and the gap is at
 * most `error`. Memory is `O(capacity)` Longs; mergeable via the Cormode/Yi rule
 * (apply the same admission policy to incoming triples). State is CAS-swapped immutables
 * so SerialMode is cheap and AtomicMode is correct under contention.
 */
class SpaceSaving(
    val capacity: Int,
    override val mode: StreamMode = defaultStreamMode,
) : DiscreteStat<HeavyHittersResult> {

    init {
        require(capacity > 0) { "capacity must be > 0" }
    }

    private class State(
        val keys: LongArray,
        val counts: LongArray,
        val errors: LongArray,
        val len: Int,
        val totalSeen: Long,
    )

    private fun emptyState() = State(
        LongArray(capacity),
        LongArray(capacity),
        LongArray(capacity),
        0,
        0L,
    )

    private val stateRef = mode.newReference(emptyState())

    private fun admit(s: State, key: Long, addCount: Long, addError: Long): State {
        for (i in 0 until s.len) {
            if (s.keys[i] == key) {
                val newCounts = s.counts.copyOf()
                val newErrors = s.errors.copyOf()
                newCounts[i] = s.counts[i] + addCount
                newErrors[i] = s.errors[i] + addError
                return State(s.keys, newCounts, newErrors, s.len, s.totalSeen + 1L)
            }
        }
        if (s.len < capacity) {
            val newKeys = s.keys.copyOf()
            val newCounts = s.counts.copyOf()
            val newErrors = s.errors.copyOf()
            newKeys[s.len] = key
            newCounts[s.len] = addCount
            newErrors[s.len] = addError
            return State(newKeys, newCounts, newErrors, s.len + 1, s.totalSeen + 1L)
        }
        var minIdx = 0
        var minCount = s.counts[0]
        for (i in 1 until s.len) {
            if (s.counts[i] < minCount) {
                minCount = s.counts[i]
                minIdx = i
            }
        }
        val newKeys = s.keys.copyOf()
        val newCounts = s.counts.copyOf()
        val newErrors = s.errors.copyOf()
        newKeys[minIdx] = key
        newCounts[minIdx] = minCount + addCount
        newErrors[minIdx] = minCount + addError
        return State(newKeys, newCounts, newErrors, s.len, s.totalSeen + 1L)
    }

    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0) return
        val w = weight.toLong()
        if (w <= 0L) return
        while (true) {
            val s = stateRef.load()
            val next = admit(s, value, w, 0L)
            if (stateRef.compareAndSet(s, next)) return
        }
    }

    override fun merge(values: HeavyHittersResult) {
        require(values.capacity == capacity) {
            "Cannot merge HeavyHitters with capacity=${values.capacity} into $capacity"
        }
        for (i in values.keys.indices) {
            val key = values.keys[i]
            val addCount = values.counts[i]
            val addError = values.errors[i]
            while (true) {
                val s = stateRef.load()
                // Replace the +1 totalSeen bump from admit with the merged batch's totalSeen
                // contribution, applied once at the end.
                val next = admit(s, key, addCount, addError)
                val withoutSeenBump = State(next.keys, next.counts, next.errors, next.len, s.totalSeen)
                if (stateRef.compareAndSet(s, withoutSeenBump)) break
            }
        }
        while (true) {
            val s = stateRef.load()
            val next = State(s.keys, s.counts, s.errors, s.len, s.totalSeen + values.totalSeen)
            if (stateRef.compareAndSet(s, next)) return
        }
    }

    override fun reset() {
        stateRef.store(emptyState())
    }

    override fun read(timestampNanos: Long): HeavyHittersResult {
        val s = stateRef.load()
        return HeavyHittersResult(
            capacity = capacity,
            keys = s.keys.copyOf(s.len),
            counts = s.counts.copyOf(s.len),
            errors = s.errors.copyOf(s.len),
            totalSeen = s.totalSeen,
        )
    }

    override fun create(mode: StreamMode?) = SpaceSaving(capacity, mode ?: this.mode)
}
