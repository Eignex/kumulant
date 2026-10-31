package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.HasObservationCount
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isNotPositiveWeight
import com.eignex.kumulant.core.preview
import com.eignex.kumulant.math.deriveChildSeed
import com.eignex.kumulant.stat.StatDefaults
import com.eignex.kumulant.stream.NoopMutex
import com.eignex.kumulant.stream.PlatformMutex
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.monotonicMode
import com.eignex.kumulant.stream.welfordLock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.pow
import kotlin.random.Random

/**
 * Reservoir sampling snapshot.
 *
 * [values] holds the retained sample (size up to `capacity`); [keys] holds the
 * parallel A-Res priority keys used to drive merging. [totalSeen] and
 * [totalWeight] count every observed update, not just retained ones.
 */
@Serializable
@SerialName("ReservoirResult")
data class ReservoirResult(
    /** Retained sample values; length up to [capacity]. */
    val values: DoubleArray,
    /** Parallel A-Res priority keys used to drive merging. */
    val keys: DoubleArray,
    /** Reservoir size. */
    val capacity: Int,
    /** Total observations the sketch has absorbed (not just retained). */
    val totalSeen: Long,
    /** Cumulative observation weight folded in. */
    val totalWeight: Double,
) : HasObservationCount {

    override val totalWeights: Double get() = totalWeight
    override fun equals(other: Any?): Boolean = other is ReservoirResult &&
        values.contentEquals(other.values) &&
        keys.contentEquals(other.keys) &&
        capacity == other.capacity &&
        totalSeen == other.totalSeen &&
        totalWeight == other.totalWeight

    override fun hashCode(): Int {
        var h = values.contentHashCode()
        h = 31 * h + keys.contentHashCode()
        h = 31 * h + capacity.hashCode()
        h = 31 * h + totalSeen.hashCode()
        h = 31 * h + totalWeight.hashCode()
        return h
    }

    override fun toString(): String = "ReservoirResult(" +
        "values=${values.preview()}, " +
        "keys=${keys.preview()}, " +
        "capacity=$capacity, " +
        "totalSeen=$totalSeen, " +
        "totalWeight=$totalWeight)"
}

/** Linear-interpolated quantile at [probability] from a reservoir sample (treats sample as unweighted). */
fun ReservoirResult.quantile(probability: Double): Double {
    require(probability in 0.0..1.0) { "Probability must be between 0.0 and 1.0" }
    if (values.isEmpty()) return Double.NaN
    val sorted = values.copyOf().also { it.sort() }
    if (sorted.size == 1) return sorted[0]
    val rank = probability * (sorted.size - 1)
    val lo = rank.toInt()
    val hi = (lo + 1).coerceAtMost(sorted.size - 1)
    val frac = rank - lo
    return sorted[lo] + frac * (sorted[hi] - sorted[lo])
}

private fun nextUp(d: Double): Double {
    if (d.isNaN() || d == Double.POSITIVE_INFINITY) return d
    if (d == 0.0) return Double.MIN_VALUE
    val bits = d.toRawBits()
    return Double.fromBits(if (d > 0.0) bits + 1 else bits - 1)
}

/** Bucket the retained sample into [binCount] equal-width bins between min and max. */
fun ReservoirResult.toSparseHistogram(binCount: Int): SparseHistogramResult {
    require(binCount > 0) { "binCount must be > 0" }
    if (values.isEmpty()) {
        return SparseHistogramResult(DoubleArray(0), DoubleArray(0), DoubleArray(0))
    }
    var lo = values[0]
    var hi = values[0]
    for (v in values) {
        if (v < lo) lo = v
        if (v > hi) hi = v
    }
    if (lo == hi) {
        // Preserve the [lower, upper) contract by widening the single-point bucket
        // by one ULP. Empty intervals would be unbucketed by consumers.
        return SparseHistogramResult(
            doubleArrayOf(lo),
            doubleArrayOf(nextUp(lo)),
            doubleArrayOf(values.size.toDouble()),
        )
    }
    val width = (hi - lo) / binCount
    val counts = DoubleArray(binCount)
    for (v in values) {
        val idx = ((v - lo) / width).toInt().coerceIn(0, binCount - 1)
        counts[idx] += 1.0
    }
    var populated = 0
    for (c in counts) if (c > 0.0) populated++
    val lowers = DoubleArray(populated)
    val uppers = DoubleArray(populated)
    val weights = DoubleArray(populated)
    var cursor = 0
    for (i in 0 until binCount) {
        if (counts[i] > 0.0) {
            lowers[cursor] = lo + i * width
            uppers[cursor] = lo + (i + 1) * width
            weights[cursor] = counts[i]
            cursor++
        }
    }
    return SparseHistogramResult(lowers, uppers, weights)
}

/**
 * Weighted reservoir sample of size [capacity] via Algorithm A-Res
 * (Efraimidis & Spirakis): each item gets a key `u^(1/w)` and the top-`k`
 * keys are retained, giving an unbiased weight-proportional sample.
 *
 * **Use cases:** keeping a representative sample of a stream you intend to
 * post-process; model training over a uniform/weighted subsample, ad-hoc
 * percentile lookups via the sample distribution, scratch-pad for
 * downstream analytics that need raw values rather than aggregates.
 *
 * **Memory:** O([capacity]) key/value slots, plus a small RNG lock.
 *
 * **Update:** O([capacity]) per admission attempt to scan for the minimum key;
 * CAS contention can require repeated scans.
 *
 * **Concurrency:** [Concurrency.Strict] and [Concurrency.HighWrite] use an outer
 * lock across updates, merges, reads, and resets. [Concurrency.Relaxed] uses CAS
 * admission of each key/value pair and a separate RNG lock; reads are not an atomic
 * snapshot across slots, and no sampling-error bound is guaranteed under contention.
 * [Concurrency.None] runs without synchronisation.
 */
class ReservoirHistogramStat(
    /** Reservoir size (capacity of retained samples). */
    val capacity: Int = StatDefaults.RESERVOIR_HISTOGRAM_CAPACITY,
    /** PRNG seed for reproducible reservoir admission. */
    val seed: Long = Random.Default.nextLong(),
    override val concurrency: Concurrency = Concurrency.None,
) : SeriesStat<ReservoirResult> {

    init {
        require(capacity > 0) { "capacity must be > 0" }
    }

    private val mode = concurrency.monotonicMode()

    private val outerLock = concurrency.welfordLock()

    // Relaxed admission has no outer lock, so its mutable RNG needs a separate one.
    private val rngLock = when (concurrency) {
        Concurrency.Relaxed, Concurrency.HighWrite -> PlatformMutex()
        else -> NoopMutex
    }
    private val random = Random(seed)

    // Copies made by `create`, so each gets its own admission stream. See `create`.
    private val copies = mode.newLong(0L)

    // Key and value live in one immutable slot swapped by a single CAS. Separate key and value cells
    // let a delayed value store from one admission overwrite the value of a later admission that
    // already replaced its key, permanently pairing a key with the wrong value.
    private class Slot(val key: Double, val value: Double)

    // Sentinel for "empty slot"; any real A-Res key (in (0, 1]) beats its key.
    private val emptySlot = Slot(Double.NEGATIVE_INFINITY, 0.0)

    private val slots = Array(capacity) { mode.newReference(emptySlot) }
    private val totalSeenCell = mode.newLong(0L)
    private val totalWeightCell = mode.newDouble(0.0)

    private fun drawKey(weight: Double): Double {
        val u = rngLock.guarded { random.nextDouble() }
        return if (weight == 1.0) u else u.pow(1.0 / weight)
    }

    private fun admit(value: Double, key: Double) {
        // Scan for the min-key slot; CAS-replace. Retry on lost CAS.
        var admitted: Slot? = null
        while (true) {
            var minIdx = 0
            var minSlot = slots[0].load()
            for (i in 1 until capacity) {
                val slot = slots[i].load()
                if (slot.key < minSlot.key) {
                    minSlot = slot
                    minIdx = i
                }
            }
            if (key <= minSlot.key) return // not admit-worthy
            val next = admitted ?: Slot(key, value).also { admitted = it }
            if (slots[minIdx].compareAndSet(minSlot, next)) return
            // Lost the CAS; re-scan against the updated state.
        }
    }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isNotPositiveWeight()) return
        outerLock.guarded {
            val key = drawKey(weight)
            admit(value, key)
            totalSeenCell.add(1L)
            totalWeightCell.add(weight)
        }
    }

    /**
     * Derives the copy's seed rather than passing this stat's own.
     *
     * A windowed reservoir builds one of these per slice, and slices sharing a seed would draw the
     * same admission keys in the same order - so whether an observation survived would turn on its
     * position within its slice rather than on its weight. The trees and forests derive for the same
     * reason; advancing a counter here is what makes each copy distinct.
     */
    override fun create(concurrency: Concurrency?) = ReservoirHistogramStat(
        capacity,
        deriveChildSeed(seed, copies.addAndGet(1L)),
        concurrency ?: this.concurrency,
    )

    override fun validateMerge(values: ReservoirResult) {
        require(values.values.size == values.keys.size) {
            "ReservoirResult values/keys size mismatch"
        }
    }

    override fun merge(values: ReservoirResult) {
        validateMerge(values)
        outerLock.guarded {
            for (i in values.values.indices) {
                admit(values.values[i], values.keys[i])
            }
            totalSeenCell.add(values.totalSeen)
            totalWeightCell.add(values.totalWeight)
        }
    }

    override fun reset() {
        outerLock.guarded {
            for (slot in slots) slot.store(emptySlot)
            totalSeenCell.store(0L)
            totalWeightCell.store(0.0)
        }
    }

    override fun read(timestampNanos: Long): ReservoirResult = outerLock.guarded {
        // Snapshot under outerLock (strict) or best-effort (relaxed). Filter
        // out unfilled slots by sentinel key.
        val outVals = DoubleArray(capacity)
        val outKeys = DoubleArray(capacity)
        var cursor = 0
        for (cell in slots) {
            val slot = cell.load()
            if (slot !== emptySlot) {
                outKeys[cursor] = slot.key
                outVals[cursor] = slot.value
                cursor++
            }
        }
        ReservoirResult(
            values = outVals.copyOf(cursor),
            keys = outKeys.copyOf(cursor),
            capacity = capacity,
            totalSeen = totalSeenCell.load(),
            totalWeight = totalWeightCell.load(),
        )
    }
}
