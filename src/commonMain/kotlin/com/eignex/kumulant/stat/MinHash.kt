package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.StreamLong
import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.concurrent.splitmix64
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.MinHashResult

/**
 * MinHash signature — for each of [numHashes] independent hash functions (salted by
 * `splitmix64(seed + i)`), maintain the running minimum hash over all inserted values.
 * The Jaccard similarity between two sets is estimated by the fraction of slots whose
 * signatures agree (see [com.eignex.kumulant.core.jaccard]).
 *
 * Standard error of the Jaccard estimate is roughly `1 / sqrt(numHashes)`. Memory is
 * [numHashes] Longs; mergeable element-wise via min when [numHashes] and [seed] match.
 */
class MinHash(
    val numHashes: Int = 128,
    val seed: Long = -3724518991637283867L, // 0xcafef00dd15ea5e5
    val mode: StreamMode = defaultStreamMode,
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
