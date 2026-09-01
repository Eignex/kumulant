package com.eignex.kumulant.stat.sketch

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.HasObservationCount
import com.eignex.kumulant.core.isNotPositiveWeight
import com.eignex.kumulant.core.preview
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.monotonicMode
import com.eignex.kumulant.stream.welfordLock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Which admission algorithm produced a [HeavyHittersResult], and so which bound its counts carry.
 *
 * The two are freely mergeable - both are `(key, count, error)` triples over the same key space - so a
 * mismatch is not refused. What it costs is tightness, and [Classic] plus [MisraGries] merges to
 * [MisraGries], the weaker of the two, on the same weakest-member rule as
 * [com.eignex.kumulant.schema.runtime.AbstractStatGroup.concurrency].
 */
@Serializable
enum class AdmissionPolicy {
    /** Space-Saving (Metwally et al. 2005). Counts never underestimate; [HeavyHittersResult.errors] bounds
     *  how far each may overestimate. */
    Classic,

    /** Lock-free Misra-Gries. Counts never overestimate; [HeavyHittersResult.deficit] bounds how far any
     *  may underestimate. */
    MisraGries,
}

/**
 * Space-Saving heavy-hitters snapshot. [keys], [counts], [errors] are parallel arrays of
 * length <= [capacity]. [totalSeen] is the unweighted update count.
 *
 * The true weighted count of a tracked key lies in `[counts[i] - errors[i], counts[i] + deficit]`. Each
 * bound closes one direction, and which one is slack depends on [policy]:
 *
 *  - Under [AdmissionPolicy.Classic] a count can only overestimate. [errors] carries the per-key bound
 *    inherited from the evicted minimum, and [deficit] is zero.
 *  - Under [AdmissionPolicy.MisraGries] a count can only underestimate, because it is only ever
 *    incremented exactly or decremented in bulk. [errors] is zero - correctly, not for want of a bound -
 *    and [deficit] carries the number of decrement rounds, which bounds the shortfall for every key.
 *
 * A merge that mixes the two accumulates [deficit] and degrades [policy], so both bounds stay sound.
 */
@Serializable
@SerialName("HeavyHittersResult")
data class HeavyHittersResult(
    val capacity: Int,
    val keys: LongArray,
    val counts: LongArray,
    val errors: LongArray,
    val totalSeen: Long,
    /** Which algorithm produced these counts, and so which direction their slack runs in. */
    val policy: AdmissionPolicy = AdmissionPolicy.Classic,
    /** Bulk-decrement rounds absorbed; bounds how far any count may fall below the true count. */
    val deficit: Long = 0L,
) : HasObservationCount {

    override val totalWeights: Double get() = totalSeen.toDouble()
    override fun equals(other: Any?): Boolean = other is HeavyHittersResult &&
        policy == other.policy &&
        deficit == other.deficit &&
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
        h = 31 * h + policy.hashCode()
        h = 31 * h + deficit.hashCode()
        return h
    }

    override fun toString(): String = "HeavyHittersResult(" +
        "capacity=$capacity, " +
        "keys=${keys.preview()}, " +
        "counts=${counts.preview()}, " +
        "errors=${errors.preview()}, " +
        "totalSeen=$totalSeen, policy=$policy, deficit=$deficit)"
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
 *    slot is claimed via CAS. Counts under this mode are **not** overestimates;
 *    they underestimate by at most the number of decrement rounds, which
 *    [HeavyHittersResult.deficit] reports. Heavy hitters still surface;
 *    small/cold keys are bled out.
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

    /**
     * Classic Space-Saving needs a consistent read of the minimum slot to evict and inherit its count,
     * which cannot be done lock-free, so [Concurrency.Relaxed] gets Misra-Gries instead.
     */
    private val policy: AdmissionPolicy =
        if (concurrency == Concurrency.Relaxed) AdmissionPolicy.MisraGries else AdmissionPolicy.Classic
    private val useMisraGries: Boolean = policy == AdmissionPolicy.MisraGries

    /**
     * Bulk-decrement rounds, plus any absorbed from a merged snapshot.
     *
     * On the Misra-Gries path this is the only thing bounding how far a count can fall below the truth.
     * It lives off the hot path: a decrement round runs only when the table is full with no free slot,
     * not on every update.
     *
     * Sticky across a policy change, because a `Classic` stat that has merged a Misra-Gries snapshot is
     * carrying that snapshot's shortfall and must keep reporting it.
     */
    private val deficitCell = mode.newLong(0L)
    private val absorbedMisraGries = mode.newLong(0L)

    private val keys = mode.newLongArray(capacity)
    private val counts = mode.newLongArray(capacity)
    private val errors = mode.newLongArray(capacity)
    private val totalSeenCell = mode.newLong(0L)

    /**
     * Classic Space-Saving admit. Caller serializes against concurrent admit / read
     * (None: trivially; Strict/HighWrite: via [outerLock]).
     */
    private fun admitClassic(key: Long, addCount: Long, addError: Long) {
        for (i in 0 until capacity) {
            if (counts.load(i) > 0L && keys.load(i) == key) {
                counts.addSaturating(i, addCount)
                errors.addSaturating(i, addError)
                return
            }
        }
        for (i in 0 until capacity) {
            if (counts.load(i) == 0L) {
                keys.store(i, key)
                counts.store(i, addCount)
                errors.store(i, addError)
                return
            }
        }
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
        counts.store(minIdx, saturatingPlus(minCount, addCount))
        errors.store(minIdx, saturatingPlus(minCount, addError))
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
                    counts.addSaturating(i, addCount)
                    errors.addSaturating(i, addError)
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

            // 3. No free slot found - best-effort subtraction of the smallest count from every count,
            //    which drives at least the slot holding that minimum to zero and so frees it for step 2
            //    on the next iteration. A concurrent admit may subtract in parallel; both progress, and
            //    step 2 accepts the resulting non-positive counts.
            //
            //    By the minimum rather than by one: a unit decrement makes this loop run once per unit
            //    of the smallest count, so admitting a single key against a table holding counts of 1e15
            //    - reachable through the weight contract, since toCounterStep caps one step at
            //    Long.MAX_VALUE/1024 - would never return. Classic Misra-Gries subtracts the minimum in
            //    one round for the same reason, which is what makes its amortised cost constant.
            var minCount = Long.MAX_VALUE
            for (i in 0 until capacity) minCount = minOf(minCount, counts.load(i))
            // At least one, so a table that a concurrent round already drove non-positive still makes
            // forward progress rather than subtracting nothing and spinning.
            val decrement = maxOf(minCount, 1L)
            for (i in 0 until capacity) counts.add(i, -decrement)
            // Every count just moved that far below where the stream put it, so the shortfall bound grows
            // by the same amount for every key. Concurrent rounds each count themselves, which is the
            // conservative direction: the bound may exceed the true shortfall, never fall short of it.
            deficitCell.add(decrement)
            // Loop and retry.
        }
    }

    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        // As in CountMinSketchStat: NaN passes `weight <= 0.0`, and rounding it lands on 1, which
        // would make a NaN weight a real observation. Counts are Long, so there is nothing to
        // propagate into.
        if (weight.isNotPositiveWeight()) return
        // Counts are Long, so a fractional weight has to be rounded. Round *up*: rounding to nearest
        // would drop everything below 0.5, so a key accumulating many small weights never surfaces
        // as a heavy hitter. Capped as in CountMinSketchStat, and for the same reason: counts are
        // Long and monotonically increasing, so an unbounded step saturates a count and the next one
        // wraps it negative, dropping a genuine heavy hitter out of the summary entirely.
        val w = weight.toCounterStep()
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

    override fun merge(values: HeavyHittersResult, workspace: com.eignex.koblas.Workspace?) {
        require(values.capacity == capacity) {
            "Cannot merge HeavyHitters with capacity=${values.capacity} into $capacity"
        }
        // A policy mismatch is not refused. Unlike a hasher mismatch, which makes two sketches'
        // arrays incomparable, both policies produce (key, count, error) triples over the same key
        // space - only the tightness differs. Refusing would also break the obvious topology of many
        // Relaxed workers feeding one Strict coordinator. So the snapshot is absorbed and the bounds
        // are widened to stay sound: the incoming shortfall is added, and the reported policy degrades
        // to the weaker of the two.
        // Guarded because reset clears both of these together and read observes both together: left
        // outside, a reset landing between them leaves an empty table reporting a degraded policy with
        // no deficit, or a deficit with no policy. Written before the counts they bound, so a reader
        // that catches the merge partway sees a bound too loose for the data rather than too tight.
        outerLock.guarded {
            if (values.policy == AdmissionPolicy.MisraGries) absorbedMisraGries.store(1L)
            deficitCell.add(values.deficit)
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
            deficitCell.store(0L)
            absorbedMisraGries.store(0L)
        }
    }

    /** This stat's own policy, weakened if it has absorbed a Misra-Gries snapshot. */
    private fun effectivePolicy(): AdmissionPolicy =
        if (useMisraGries || absorbedMisraGries.load() != 0L) AdmissionPolicy.MisraGries else policy

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
            policy = effectivePolicy(),
            deficit = deficitCell.load(),
        )
    }

    override fun create(concurrency: Concurrency?) = SpaceSavingStat(capacity, concurrency ?: this.concurrency)
}
