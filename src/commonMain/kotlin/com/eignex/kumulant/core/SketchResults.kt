package com.eignex.kumulant.core

import com.eignex.kumulant.concurrent.splitmix64
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
 * Bloom-filter snapshot. [words] is the bitset packed as `bits / 64` longs; merging two
 * snapshots requires identical [bits] and [hashes].
 */
@Serializable
@SerialName("BloomFilter")
data class BloomFilterResult(
    val bits: Int,
    val hashes: Int,
    val words: LongArray,
    val totalSeen: Long,
) : Result

/** True iff every bit set during an `update(value)` is still set in [words]. */
fun BloomFilterResult.contains(value: Long): Boolean {
    val mask = (bits - 1).toLong()
    val h1 = splitmix64(value)
    val h2 = splitmix64(h1)
    for (i in 0 until hashes) {
        val pos = (h1 + i.toLong() * h2) and mask
        val wordIdx = (pos ushr 6).toInt()
        val bitMask = 1L shl (pos and 63L).toInt()
        if (words[wordIdx] and bitMask == 0L) return false
    }
    return true
}

/**
 * MinHash signature snapshot. [signatures] is the per-hash running minimum of
 * `splitmix64(value xor splitmix64(seed + i))` over all updates; merging two snapshots
 * takes element-wise min and requires identical [numHashes] and [seed].
 */
@Serializable
@SerialName("MinHash")
data class MinHashResult(
    val numHashes: Int,
    val seed: Long,
    val signatures: LongArray,
    val totalSeen: Long,
) : Result

/**
 * Estimated Jaccard similarity between the two underlying sets — the fraction of slots
 * where signatures agree. Requires matching [numHashes] and [seed].
 */
fun MinHashResult.jaccard(other: MinHashResult): Double {
    require(numHashes == other.numHashes) {
        "Cannot compare MinHash with numHashes=${other.numHashes} to $numHashes"
    }
    require(seed == other.seed) {
        "Cannot compare MinHash with seed=${other.seed} to $seed"
    }
    if (numHashes == 0) return 0.0
    var matches = 0
    var populated = 0
    for (i in 0 until numHashes) {
        val a = signatures[i]
        val b = other.signatures[i]
        if (a == Long.MAX_VALUE && b == Long.MAX_VALUE) continue
        populated++
        if (a == b) matches++
    }
    return if (populated == 0) 0.0 else matches.toDouble() / populated.toDouble()
}
