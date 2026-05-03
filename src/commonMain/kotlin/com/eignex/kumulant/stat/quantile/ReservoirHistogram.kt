package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.defaultStreamMode
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
@SerialName("Reservoir")
data class ReservoirResult(
    val values: DoubleArray,
    val keys: DoubleArray,
    val capacity: Int,
    val totalSeen: Long,
    val totalWeight: Double
) : Result

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
        return SparseHistogramResult(
            doubleArrayOf(lo),
            doubleArrayOf(lo),
            doubleArrayOf(values.size.toDouble())
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
 * State is held behind a CAS-swapped reference; under [com.eignex.kumulant.stream.AtomicMode]
 * concurrent updates are racy but eventually consistent. For strict
 * thread-safety, wrap in `.locked()`.
 */
class ReservoirHistogram(
    val capacity: Int = 1024,
    val seed: Long = Random.Default.nextLong(),
    override val mode: StreamMode = defaultStreamMode,
) : SeriesStat<ReservoirResult> {

    init {
        require(capacity > 0) { "capacity must be > 0" }
    }

    private class State(
        val values: DoubleArray,
        val keys: DoubleArray,
        val totalSeen: Long,
        val totalWeight: Double
    )

    private val random = Random(seed)
    private val stateRef = mode.newReference(
        State(DoubleArray(0), DoubleArray(0), 0L, 0.0)
    )

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0 || value.isNaN()) return

        val u = random.nextDouble()
        val key = if (weight == 1.0) u else u.pow(1.0 / weight)

        while (true) {
            val s = stateRef.load()
            val newState: State
            if (s.values.size < capacity) {
                val newVals = s.values.copyOf(s.values.size + 1)
                val newKeys = s.keys.copyOf(s.keys.size + 1)
                newVals[s.values.size] = value
                newKeys[s.keys.size] = key
                newState = State(newVals, newKeys, s.totalSeen + 1, s.totalWeight + weight)
            } else {
                var minIdx = 0
                var minKey = s.keys[0]
                for (i in 1 until s.keys.size) {
                    if (s.keys[i] < minKey) {
                        minKey = s.keys[i]
                        minIdx = i
                    }
                }
                if (key > minKey) {
                    val newVals = s.values.copyOf()
                    val newKeys = s.keys.copyOf()
                    newVals[minIdx] = value
                    newKeys[minIdx] = key
                    newState = State(newVals, newKeys, s.totalSeen + 1, s.totalWeight + weight)
                } else {
                    newState = State(s.values, s.keys, s.totalSeen + 1, s.totalWeight + weight)
                }
            }
            if (stateRef.compareAndSet(s, newState)) return
        }
    }

    override fun create(mode: StreamMode?) = ReservoirHistogram(
        capacity,
        seed,
        mode ?: this.mode
    )

    override fun merge(values: ReservoirResult) {
        require(values.values.size == values.keys.size) {
            "ReservoirResult values/keys size mismatch"
        }
        while (true) {
            val s = stateRef.load()
            val combinedVals = s.values + values.values
            val combinedKeys = s.keys + values.keys

            val (vOut, kOut) = if (combinedVals.size <= capacity) {
                combinedVals to combinedKeys
            } else {
                val indices = (combinedKeys.indices).sortedByDescending { combinedKeys[it] }
                    .take(capacity)
                val v = DoubleArray(capacity) { combinedVals[indices[it]] }
                val k = DoubleArray(capacity) { combinedKeys[indices[it]] }
                v to k
            }
            val newState = State(
                vOut,
                kOut,
                s.totalSeen + values.totalSeen,
                s.totalWeight + values.totalWeight
            )
            if (stateRef.compareAndSet(s, newState)) return
        }
    }

    override fun reset() {
        stateRef.store(State(DoubleArray(0), DoubleArray(0), 0L, 0.0))
    }

    override fun read(timestampNanos: Long): ReservoirResult {
        val s = stateRef.load()
        return ReservoirResult(
            values = s.values.copyOf(),
            keys = s.keys.copyOf(),
            capacity = capacity,
            totalSeen = s.totalSeen,
            totalWeight = s.totalWeight
        )
    }
}
