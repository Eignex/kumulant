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

/**
 * MinHash signature — for each of [numHashes] independent hash functions (salted by
 * `splitmix64(seed + i)`), maintain the running minimum hash over all inserted values.
 * The Jaccard similarity between two sets is estimated by the fraction of slots whose
 * signatures agree (see [jaccard]).
 *
 * Standard error of the Jaccard estimate is roughly `1 / sqrt(numHashes)`. Memory is
 * [numHashes] Longs; mergeable element-wise via min when [numHashes] and [seed] match.
 */
class MinHash(
    val numHashes: Int = 128,
    val seed: Long = -3724518991637283867L, // 0xcafef00dd15ea5e5
    override val mode: StreamMode = defaultStreamMode,
) : DiscreteStat<MinHashResult> {

    init {
        require(numHashes > 0) { "numHashes must be > 0" }
    }

    private val salts: LongArray = LongArray(numHashes) { splitmix64(seed + it.toLong()) }
    private val signatures: Array<StreamLong> = Array(numHashes) { mode.newLong(Long.MAX_VALUE) }
    private val totalSeen: StreamLong = mode.newLong(0L)

    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0) return
        for (i in 0 until numHashes) {
            casMin(signatures[i], splitmix64(value xor salts[i]))
        }
        totalSeen.add(1L)
    }

    private fun casMin(cell: StreamLong, candidate: Long) {
        while (true) {
            val current = cell.load()
            if (candidate >= current) return
            if (cell.compareAndSet(current, candidate)) return
        }
    }

    override fun merge(values: MinHashResult) {
        require(values.numHashes == numHashes && values.seed == seed) {
            "Cannot merge MinHash with (numHashes=${values.numHashes}, seed=${values.seed}) into (numHashes=$numHashes, seed=$seed)"
        }
        for (i in signatures.indices) {
            val incoming = values.signatures[i]
            if (incoming != Long.MAX_VALUE) casMin(signatures[i], incoming)
        }
        totalSeen.add(values.totalSeen)
    }

    override fun reset() {
        for (cell in signatures) cell.store(Long.MAX_VALUE)
        totalSeen.store(0L)
    }

    override fun read(timestampNanos: Long): MinHashResult {
        val snapshot = LongArray(numHashes) { signatures[it].load() }
        return MinHashResult(
            numHashes = numHashes,
            seed = seed,
            signatures = snapshot,
            totalSeen = totalSeen.load(),
        )
    }

    override fun create(mode: StreamMode?) = MinHash(numHashes, seed, mode ?: this.mode)
}
