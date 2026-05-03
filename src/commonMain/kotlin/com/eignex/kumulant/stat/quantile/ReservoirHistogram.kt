package com.eignex.kumulant.stat.quantile

import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.CasLock
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
 * Concurrency: all `update`/`merge`/`read`/`reset` calls are internally serialized
 * via a private CAS spin-mutex. Safe under any [StreamMode]; throughput-bound
 * under thread contention.
 */
class ReservoirHistogram(
    val capacity: Int = 1024,
    val seed: Long = Random.Default.nextLong(),
    override val mode: StreamMode = defaultStreamMode,
) : SeriesStat<ReservoirResult> {

    init {
        require(capacity > 0) { "capacity must be > 0" }
    }

    // All mutable state is accessed only under [lock].
    private val random = Random(seed)
    private val values = DoubleArray(capacity)
    private val keys = DoubleArray(capacity)
    private var len = 0
    private var totalSeen = 0L
    private var totalWeight = 0.0
    private val lock = CasLock(mode)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0 || value.isNaN()) return
        lock.withLock {
            val u = random.nextDouble()
            val key = if (weight == 1.0) u else u.pow(1.0 / weight)
            admit(value, key)
            totalSeen++
            totalWeight += weight
        }
    }

    private fun admit(value: Double, key: Double) {
        val n = len
        if (n < capacity) {
            values[n] = value
            keys[n] = key
            len = n + 1
            return
        }
        var minIdx = 0
        var minKey = keys[0]
        for (i in 1 until capacity) {
            if (keys[i] < minKey) {
                minKey = keys[i]
                minIdx = i
            }
        }
        if (key > minKey) {
            values[minIdx] = value
            keys[minIdx] = key
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
        lock.withLock {
            for (i in values.values.indices) {
                admit(values.values[i], values.keys[i])
            }
            totalSeen += values.totalSeen
            totalWeight += values.totalWeight
        }
    }

    override fun reset() {
        lock.withLock {
            len = 0
            totalSeen = 0L
            totalWeight = 0.0
            for (i in 0 until capacity) {
                values[i] = 0.0
                keys[i] = 0.0
            }
        }
    }

    override fun read(timestampNanos: Long): ReservoirResult = lock.withLock {
        val n = len.coerceAtMost(capacity)
        ReservoirResult(
            values = values.copyOf(n),
            keys = keys.copyOf(n),
            capacity = capacity,
            totalSeen = totalSeen,
            totalWeight = totalWeight
        )
    }
}
