package com.eignex.kumulant.stat.sketch

import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.stream.StreamLong
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.defaultStreamMode
import com.eignex.kumulant.stream.splitmix64
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Count-Min sketch snapshot. [counters] is the [depth] × [width] matrix of counters in
 * row-major order. [seed] determines the per-row hash salts; merging two snapshots
 * requires identical [depth], [width], and [seed]. [totalSeen] is the unweighted update
 * count.
 */
@Serializable
@SerialName("CountMinSketch")
data class CountMinSketchResult(
    val depth: Int,
    val width: Int,
    val seed: Long,
    val counters: LongArray,
    val totalSeen: Long,
) : Result

/** Estimated weighted count of [value] — the minimum across rows. */
fun CountMinSketchResult.estimate(value: Long): Long {
    if (counters.isEmpty()) return 0L
    val mask = (width - 1).toLong()
    var min = Long.MAX_VALUE
    for (row in 0 until depth) {
        val salt = splitmix64(seed + row.toLong())
        val idx = (splitmix64(value xor salt) and mask).toInt()
        val c = counters[row * width + idx]
        if (c < min) min = c
    }
    return if (min == Long.MAX_VALUE) 0L else min
}

/**
 * Count-Min sketch — a probabilistic frequency estimator over a [depth] × [width] matrix
 * of counters. Each update hashes the value with [depth] independent salts (derived from
 * [seed]) and increments one counter per row; the estimated count for any value is the
 * minimum counter across rows.
 *
 * Estimates are one-sided overestimates: `estimate(x) >= true count(x)` always, with the
 * overestimate bounded by `2 * totalSeen / width` with high probability over the salt
 * choice. Memory is `depth * width` Longs; mergeable element-wise when [depth], [width],
 * and [seed] match.
 *
 * [width] must be a power of two so that the hash maps to an index via masking.
 */
class CountMinSketch(
    val depth: Int = 5,
    val width: Int = 1024,
    val seed: Long = -7046029254386353133L, // 0x9e3779b97f4a7c15
    override val mode: StreamMode = defaultStreamMode,
) : DiscreteStat<CountMinSketchResult> {

    init {
        require(depth > 0) { "depth must be > 0" }
        require(width > 0) { "width must be > 0" }
        require(width and (width - 1) == 0) { "width must be a power of two" }
    }

    private val mask: Long = (width - 1).toLong()
    private val rowSalts: LongArray = LongArray(depth) { splitmix64(seed + it.toLong()) }
    private val counters: Array<StreamLong> = Array(depth * width) { mode.newLong(0L) }
    private val totalSeen: StreamLong = mode.newLong(0L)

    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0) return
        val w = kotlin.math.round(weight).toLong()
        if (w <= 0L) return
        for (row in 0 until depth) {
            val idx = (splitmix64(value xor rowSalts[row]) and mask).toInt()
            counters[row * width + idx].add(w)
        }
        totalSeen.add(1L)
    }

    override fun merge(values: CountMinSketchResult) {
        require(values.depth == depth && values.width == width && values.seed == seed) {
            "Cannot merge CountMinSketch with shape (${values.depth}, ${values.width}, seed=${values.seed}) into ($depth, $width, seed=$seed)"
        }
        for (i in counters.indices) {
            val incoming = values.counters[i]
            if (incoming != 0L) counters[i].add(incoming)
        }
        totalSeen.add(values.totalSeen)
    }

    override fun reset() {
        for (cell in counters) cell.store(0L)
        totalSeen.store(0L)
    }

    override fun read(timestampNanos: Long): CountMinSketchResult {
        val snapshot = LongArray(counters.size) { counters[it].load() }
        return CountMinSketchResult(
            depth = depth,
            width = width,
            seed = seed,
            counters = snapshot,
            totalSeen = totalSeen.load(),
        )
    }

    override fun create(mode: StreamMode?) = CountMinSketch(depth, width, seed, mode ?: this.mode)
}
