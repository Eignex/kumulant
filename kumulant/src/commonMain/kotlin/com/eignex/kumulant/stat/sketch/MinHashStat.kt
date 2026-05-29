package com.eignex.kumulant.stat.sketch

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.math.LongHasher
import com.eignex.kumulant.math.SplitMix64
import com.eignex.kumulant.math.splitmix64
import com.eignex.kumulant.stream.StreamLong
import com.eignex.kumulant.stream.StreamLongArray
import com.eignex.kumulant.stream.casMin
import com.eignex.kumulant.stream.monotonicMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * MinHash signature snapshot. [signatures] is the per-hash running minimum of
 * `splitmix64(value xor splitmix64(seed + i))` over all updates; merging two snapshots
 * takes element-wise min and requires identical `numHashes` and `seed`.
 */
@Serializable
@SerialName("MinHashResult")
data class MinHashResult(
    /** Number of independent hash functions in the signature. */
    val numHashes: Int,
    /** PRNG seed used to derive the per-hash salts; must match for merge / [jaccard]. */
    val seed: Long,
    /** Per-hash running minimums; element-wise min produces a valid merge. */
    val signatures: LongArray,
    /** Number of `update(value)` calls absorbed; informational. */
    val totalSeen: Long,
) : Result

/**
 * Estimated Jaccard similarity between the two underlying sets - the fraction of slots
 * where signatures agree. Requires matching `numHashes` and `seed`.
 */
fun MinHashResult.jaccard(other: MinHashResult): Double {
    require(numHashes == other.numHashes) {
        "Cannot compare MinHashStat with numHashes=${other.numHashes} to $numHashes"
    }
    require(seed == other.seed) {
        "Cannot compare MinHashStat with seed=${other.seed} to $seed"
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
 * MinHash signature - for each of `numHashes` independent hash functions (salted by
 * `splitmix64(seed + i)`), maintain the running minimum hash over all inserted values.
 * The Jaccard similarity between two sets is estimated by the fraction of slots whose
 * signatures agree (see [jaccard]).
 *
 * Standard error of the Jaccard estimate is roughly `1 / sqrt(numHashes)`.
 *
 * **Use cases:** set-similarity estimation under bounded memory; near-duplicate
 * detection, recommender-style "users who like X also like Y", clustering
 * coarse buckets via locality-sensitive hashing.
 *
 * **Memory:** O([numHashes]) Longs.
 *
 * **Update:** O([numHashes]) per observation; one hash per signature slot
 * followed by a CAS-min loop.
 *
 * **Concurrency:** Per-slot single-cell CAS-min loop on an atomic Long array.
 * Lock-free and exact under every [Concurrency] level; min over an unordered
 * set is the same regardless of writer order.
 */
class MinHashStat(
    /** Signature length; higher = better Jaccard accuracy at more memory. */
    val numHashes: Int = 128,
    /** PRNG seed used to derive the per-hash salts; must match across instances to compare. */
    val seed: Long = -3724518991637283867L, // 0xcafef00dd15ea5e5
    /** Mixer applied to each `value xor salt`; defaults to [SplitMix64]. */
    val hasher: LongHasher = SplitMix64,
    override val concurrency: Concurrency = Concurrency.None,
) : DiscreteStat<MinHashResult> {

    init {
        require(numHashes > 0) { "numHashes must be > 0" }
    }

    private val salts: LongArray = LongArray(numHashes) { splitmix64(seed + it.toLong()) }
    private val mode = concurrency.monotonicMode()
    private val signatures: StreamLongArray = mode.newLongArray(numHashes) { Long.MAX_VALUE }
    private val totalSeen: StreamLong = mode.newLong(0L)

    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0) return
        for (i in 0 until numHashes) {
            casMin(signatures, i, hasher.mix(value xor salts[i]))
        }
        totalSeen.add(1L)
    }

    override fun merge(values: MinHashResult) {
        require(values.numHashes == numHashes && values.seed == seed) {
            "Cannot merge MinHashStat with (numHashes=${values.numHashes}, seed=${values.seed}) " +
                "into (numHashes=$numHashes, seed=$seed)"
        }
        for (i in 0 until numHashes) {
            val incoming = values.signatures[i]
            if (incoming != Long.MAX_VALUE) casMin(signatures, i, incoming)
        }
        totalSeen.add(values.totalSeen)
    }

    override fun reset() {
        for (i in 0 until numHashes) signatures.store(i, Long.MAX_VALUE)
        totalSeen.store(0L)
    }

    override fun read(timestampNanos: Long): MinHashResult {
        val snapshot = LongArray(numHashes) { signatures.load(it) }
        return MinHashResult(
            numHashes = numHashes,
            seed = seed,
            signatures = snapshot,
            totalSeen = totalSeen.load(),
        )
    }

    override fun create(concurrency: Concurrency?) = MinHashStat(numHashes, seed, hasher, concurrency ?: this.concurrency)
}
