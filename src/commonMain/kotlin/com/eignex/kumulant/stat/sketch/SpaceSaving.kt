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
 * (apply the same admission policy to incoming triples).
 */
class SpaceSaving(
    val capacity: Int,
    override val mode: StreamMode = defaultStreamMode,
) : DiscreteStat<HeavyHittersResult> {

    init {
        require(capacity > 0) { "capacity must be > 0" }
    }

    private val keys = LongArray(capacity)
    private val counts = LongArray(capacity)
    private val errors = LongArray(capacity)
    private val len = mode.newLong(0L)
    private val totalSeen = mode.newLong(0L)

    private fun admit(key: Long, addCount: Long, addError: Long) {
        val n = len.load().toInt()
        for (i in 0 until n) {
            if (keys[i] == key) {
                counts[i] += addCount
                errors[i] += addError
                return
            }
        }
        if (n < capacity) {
            keys[n] = key
            counts[n] = addCount
            errors[n] = addError
            len.store((n + 1).toLong())
            return
        }
        var minIdx = 0
        var minCount = counts[0]
        for (i in 1 until n) {
            if (counts[i] < minCount) {
                minCount = counts[i]
                minIdx = i
            }
        }
        keys[minIdx] = key
        counts[minIdx] = minCount + addCount
        errors[minIdx] = minCount + addError
    }

    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0) return
        val w = weight.toLong()
        if (w <= 0L) return
        admit(value, w, 0L)
        totalSeen.add(1L)
    }

    override fun merge(values: HeavyHittersResult) {
        require(values.capacity == capacity) {
            "Cannot merge HeavyHitters with capacity=${values.capacity} into $capacity"
        }
        for (i in values.keys.indices) {
            admit(values.keys[i], values.counts[i], values.errors[i])
        }
        totalSeen.add(values.totalSeen)
    }

    override fun reset() {
        len.store(0L)
        totalSeen.store(0L)
        for (i in 0 until capacity) {
            keys[i] = 0L
            counts[i] = 0L
            errors[i] = 0L
        }
    }

    override fun read(timestampNanos: Long): HeavyHittersResult {
        val n = len.load().toInt().coerceAtMost(capacity)
        return HeavyHittersResult(
            capacity = capacity,
            keys = keys.copyOf(n),
            counts = counts.copyOf(n),
            errors = errors.copyOf(n),
            totalSeen = totalSeen.load(),
        )
    }

    override fun create(mode: StreamMode?) = SpaceSaving(capacity, mode ?: this.mode)
}
