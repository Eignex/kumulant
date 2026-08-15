package com.eignex.kumulant.stat.sketch

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.HasObservationCount
import com.eignex.kumulant.core.preview
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.monotonicMode
import com.eignex.kumulant.stream.welfordLock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.ceil

/**
 * Space-Saving heavy-hitters snapshot. [keys], [counts], [errors] are parallel arrays of
 * length <= [capacity]; for each tracked key, [counts] is the (over)estimated weighted
 * count and [errors] is the Space-Saving overestimate bound (the count is at most this
 * much above the true count). [totalSeen] is the unweighted update count.
 */
@Serializable
@SerialName("HeavyHittersResult")
data class HeavyHittersResult(
    val capacity: Int,
    val keys: LongArray,
    val counts: LongArray,
    val errors: LongArray,
    val totalSeen: Long,
) : HasObservationCount {

    override val totalWeights: Double get() = totalSeen.toDouble()
    override fun equals(other: Any?): Boolean = other is HeavyHittersResult &&
        capacity == other.capacity &&
        keys.contentEquals(other.keys) &&
        counts.contentEquals(other.counts) &&
        errors.contentEquals(other.errors) &&
        totalSeen == other.totalSeen

    override fun hashCode(): Int {
        var h = capacity.hashCode()
        h = 31 * h + keys.contentHashCode()
        h = 31 * h + counts.contentHashCode()
        h = 31 * h + errors.contentHashCode()
        h = 31 * h + totalSeen.hashCode()
        return h
    }

    override fun toString(): String = "HeavyHittersResult(" +
        "capacity=$capacity, " +
        "keys=${keys.preview()}, " +
        "counts=${counts.preview()}, " +
        "errors=${errors.preview()}, " +
        "totalSeen=$totalSeen)"
}

/**
 * Heavy-hitters tracker; keeps the [capacity] most-frequent keys with
 * one-sided overestimate guarantees on their counts.
 *
 * **Use cases:** top-k key discovery under bounded memory; most-frequent
 * users, top error fingerprints, hot cache keys. Pair with [CountMinSketchStat]
 * if you need point-frequency lookups on arbitrary keys, not just the top set.
 *
 * **Memory:** O([capacity]) Longs (keys + counts + errors).
 *
 * **Update:** O(1) per observation when the key is tracked; O([capacity]) on
 * a miss when the table is full (linear scan to find the minimum-count slot).
 *
 * **Concurrency:** Two algorithms run depending on level:
 *
 *  - [Concurrency.None], [Concurrency.Strict], [Concurrency.HighWrite]:
 *    classic Space-Saving (Metwally, Agrawal, El Abbadi 2005). On a miss when
 *    full, the minimum-count slot is evicted and the new key inherits the old
 *    count plus its weight, with the old count recorded as the new key's
 *    overestimate bound. Reported counts are one-sided overestimates:
 *    `count >= true count`, gap <= `error`. Strict/HighWrite serialise against
 *    reads/merges via an outer lock.
 *
 *  - [Concurrency.Relaxed]: lock-free Misra-Gries variant. On a miss when
 *    full, all counts are decremented by one in best-effort fashion; a freed
 *    slot is claimed via CAS. Counts under this mode are **not** overestimates
 *   ; they may underestimate by the number of decrements, and the classic
 *    overestimate bound does not hold. Heavy hitters still surface; small/cold
 *    keys are bled out.
 */
class SpaceSavingStat(
    /** Maximum number of distinct keys tracked; smaller capacity means looser
     *  heavy-hitter guarantees but lower memory. */
    val capacity: Int,
    override val concurrency: Concurrency = Concurrency.None,
) : DiscreteStat<HeavyHittersResult> {

    init {
        require(capacity > 0) { "capacity must be > 0" }
    }

    private val mode = concurrency.monotonicMode()

    // Noop under None/Relaxed; real under Strict/HighWrite.
    private val outerLock = concurrency.welfordLock()
    private val useMisraGries: Boolean = concurrency == Concurrency.Relaxed

    private val keys = mode.newLongArray(capacity)
    private val counts = mode.newLongArray(capacity)
    private val errors = mode.newLongArray(capacity)
    private val totalSeenCell = mode.newLong(0L)

    /**
     * Classic Space-Saving admit. Caller serializes against concurrent admit / read
     * (None: trivially; Strict/HighWrite: via [outerLock]).
     */
    private fun admitClassic(key: Long, addCount: Long, addError: Long) {
        // Match existing key
        for (i in 0 until capacity) {
            if (counts.load(i) > 0L && keys.load(i) == key) {
                counts.add(i, addCount)
                errors.add(i, addError)
                return
            }
        }
        // Find empty slot (count == 0)
        for (i in 0 until capacity) {
            if (counts.load(i) == 0L) {
                keys.store(i, key)
                counts.store(i, addCount)
                errors.store(i, addError)
                return
            }
        }
        // Evict min-count slot
        var minIdx = 0
        var minCount = counts.load(0)
        for (i in 1 until capacity) {
            val c = counts.load(i)
            if (c < minCount) {
                minCount = c
                minIdx = i
            }
        }
        keys.store(minIdx, key)
        counts.store(minIdx, minCount + addCount)
        errors.store(minIdx, minCount + addError)
    }

    /**
     * Lock-free Misra-Gries admit. Bounded drift: concurrent racers may
     * see a brief torn (newCount, staleKey) view in a freshly-claimed slot.
     */
    private fun admitMisraGries(key: Long, addCount: Long, addError: Long) {
        while (true) {
            // 1. Find a slot whose current (key, count) matches and increment in place.
            var matched = false
            for (i in 0 until capacity) {
                if (counts.load(i) > 0L && keys.load(i) == key) {
                    counts.add(i, addCount)
                    errors.add(i, addError)
                    matched = true
                    break
                }
            }
            if (matched) return

            // 2. Try to claim a free slot (count <= 0) via CAS on the count.
            //    Concurrent decrements in step 3 can drive counts negative; treat
            //    those as available too so an over-decrement doesn't trap callers.
            var claimed = false
            for (i in 0 until capacity) {
                val c = counts.load(i)
                if (c <= 0L) {
                    if (counts.compareAndSet(i, c, addCount)) {
                        keys.store(i, key)
                        errors.store(i, addError)
                        claimed = true
                        break
                    }
                }
            }
            if (claimed) return

            // 3. No free slot found - best-effort decrement of every count. A
            //    concurrent admit may decrement in parallel; both progress, and
            //    step 2 on the next iteration accepts the resulting non-positive
            //    counts.
            for (i in 0 until capacity) counts.add(i, -1L)
            // Loop and retry.
        }
    }

    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0) return
        // Counts are Long, so a fractional weight has to be rounded. Round *up*: rounding to nearest
        // dropped everything below 0.5 outright, so a key accumulating many small weights never
        // appeared among the heavy hitters at all. Counts are upper bounds as a result.
        val w = ceil(weight).toLong().coerceAtLeast(1L)
        if (useMisraGries) {
            admitMisraGries(value, w, 0L)
            totalSeenCell.add(1L)
        } else {
            outerLock.guarded {
                admitClassic(value, w, 0L)
                totalSeenCell.add(1L)
            }
        }
    }

    override fun merge(values: HeavyHittersResult) {
        require(values.capacity == capacity) {
            "Cannot merge HeavyHitters with capacity=${values.capacity} into $capacity"
        }
        if (useMisraGries) {
            for (i in values.keys.indices) {
                admitMisraGries(values.keys[i], values.counts[i], values.errors[i])
            }
            totalSeenCell.add(values.totalSeen)
        } else {
            outerLock.guarded {
                for (i in values.keys.indices) {
                    admitClassic(values.keys[i], values.counts[i], values.errors[i])
                }
                totalSeenCell.add(values.totalSeen)
            }
        }
    }

    override fun reset() {
        outerLock.guarded {
            for (i in 0 until capacity) {
                keys.store(i, 0L)
                counts.store(i, 0L)
                errors.store(i, 0L)
            }
            totalSeenCell.store(0L)
        }
    }

    override fun read(timestampNanos: Long): HeavyHittersResult = outerLock.guarded {
        // Filter active slots (count > 0). Snapshot per-slot; under Relaxed a brief
        // torn pair window is possible.
        var active = 0
        for (i in 0 until capacity) {
            if (counts.load(i) > 0L) active++
        }
        val outK = LongArray(active)
        val outC = LongArray(active)
        val outE = LongArray(active)
        var cursor = 0
        for (i in 0 until capacity) {
            val c = counts.load(i)
            if (c > 0L && cursor < active) {
                outK[cursor] = keys.load(i)
                outC[cursor] = c
                outE[cursor] = errors.load(i)
                cursor++
            }
        }
        HeavyHittersResult(
            capacity = capacity,
            keys = outK.copyOf(cursor),
            counts = outC.copyOf(cursor),
            errors = outE.copyOf(cursor),
            totalSeen = totalSeenCell.load(),
        )
    }

    override fun create(concurrency: Concurrency?) = SpaceSavingStat(capacity, concurrency ?: this.concurrency)
}
