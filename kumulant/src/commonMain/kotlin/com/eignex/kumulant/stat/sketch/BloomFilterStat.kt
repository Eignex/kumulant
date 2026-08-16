package com.eignex.kumulant.stat.sketch

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.HasObservationCount
import com.eignex.kumulant.core.preview
import com.eignex.kumulant.math.HasherRef
import com.eignex.kumulant.math.Hashers
import com.eignex.kumulant.math.LongHasher
import com.eignex.kumulant.math.SplitMix64
import com.eignex.kumulant.stream.StreamLong
import com.eignex.kumulant.stream.StreamLongArray
import com.eignex.kumulant.stream.casOr
import com.eignex.kumulant.stream.monotonicMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Bloom-filter snapshot. `words` is the bitset packed as `bits / 64` longs; merging two
 * snapshots requires identical [bits] and [hashes].
 */
@Serializable
@SerialName("BloomFilterResult")
data class BloomFilterResult(
    /** Total bitset size in bits. */
    val bits: Int,
    /** Number of hash functions probed per membership query. */
    val hashes: Int,
    /** Packed bitset (`bits / 64` longs); mergeable via word-wise OR. */
    val words: LongArray,
    /** Number of `update(value)` calls absorbed; informational. */
    val totalSeen: Long,
    /** Reference to the [LongHasher] that produced the bitset; resolved by [contains]. */
    val hasher: HasherRef = HasherRef.SplitMix64,
) : HasObservationCount {

    override val totalWeights: Double get() = totalSeen.toDouble()
    override fun equals(other: Any?): Boolean = other is BloomFilterResult &&
        bits == other.bits &&
        hashes == other.hashes &&
        words.contentEquals(other.words) &&
        totalSeen == other.totalSeen &&
        hasher == other.hasher

    override fun hashCode(): Int {
        var h = bits.hashCode()
        h = 31 * h + hashes.hashCode()
        h = 31 * h + words.contentHashCode()
        h = 31 * h + totalSeen.hashCode()
        h = 31 * h + hasher.hashCode()
        return h
    }

    override fun toString(): String =
        "BloomFilterResult(bits=$bits, hashes=$hashes, words=${words.preview()}, totalSeen=$totalSeen, hasher=$hasher)"
}

/**
 * True iff every bit set during an `update(value)` is still set in `words`. Re-derives the
 * bit positions with the same [LongHasher] the filter used (resolved by name via [Hashers]),
 * so a custom hasher must be registered before querying.
 */
fun BloomFilterResult.contains(value: Long): Boolean {
    val mask = (bits - 1).toLong()
    val mixer = Hashers.resolve(hasher)
    val h1 = mixer.mix(value)
    val h2 = mixer.mix(h1)
    for (i in 0 until hashes) {
        val pos = (h1 + i.toLong() * h2) and mask
        val wordIdx = (pos ushr 6).toInt()
        val bitMask = 1L shl (pos and 63L).toInt()
        if (words[wordIdx] and bitMask == 0L) return false
    }
    return true
}

/**
 * Bloom filter - probabilistic set-membership test with no false negatives. [bits] bits
 * are split across [hashes] positions per insert, derived from the [hasher] (default
 * [SplitMix64]) via the Kirsch-Mitzenmacher double-hashing scheme.
 *
 * False-positive rate is approximately `(1 - e^(-hashes * n / bits))^hashes` where `n` is
 * the number of distinct inserts. Memory is `bits / 64` Longs; mergeable element-wise via
 * bitwise OR when [bits] and [hashes] match.
 *
 * [bits] must be a power of two and a multiple of 64.
 *
 * **Use cases:** membership queries with bounded memory; feature flags,
 * deduplication of seen keys, "have we seen this user before". Tolerates
 * false positives but never false negatives.
 *
 * **Memory:** O(bits / 64) Longs, plus a `totalSeen` counter.
 *
 * **Update:** O([hashes]) per observation; [hashes] independent atomic OR ops.
 *
 * **Concurrency:** Atomic OR on a striped Long array. Lock-free and exact
 * under every [Concurrency] level; bit sets are idempotent and commutative.
 */
class BloomFilterStat(
    val bits: Int = 1 shl 16,
    val hashes: Int = 7,
    /** Mixer seeding the double-hashing scheme; defaults to [SplitMix64]. */
    val hasher: LongHasher = SplitMix64,
    override val concurrency: Concurrency = Concurrency.None,
) : DiscreteStat<BloomFilterResult> {

    init {
        require(bits > 0) { "bits must be > 0" }
        require(bits and (bits - 1) == 0) { "bits must be a power of two" }
        require(bits % 64 == 0) { "bits must be a multiple of 64" }
        require(hashes > 0) { "hashes must be > 0" }
    }

    private val hasherRef: HasherRef = HasherRef(hasher.name)
    private val wordCount: Int = bits / 64
    private val mask: Long = (bits - 1).toLong()
    private val mode = concurrency.monotonicMode()
    private val words: StreamLongArray = mode.newLongArray(wordCount)
    private val totalSeen: StreamLong = mode.newLong(0L)

    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0 || weight.isNaN()) return
        val h1 = hasher.mix(value)
        val h2 = hasher.mix(h1)
        for (i in 0 until hashes) {
            val pos = (h1 + i.toLong() * h2) and mask
            val wordIdx = (pos ushr 6).toInt()
            val bitMask = 1L shl (pos and 63L).toInt()
            casOr(words, wordIdx, bitMask)
        }
        totalSeen.add(1L)
    }

    override fun merge(values: BloomFilterResult) {
        require(values.bits == bits && values.hashes == hashes) {
            "Cannot merge BloomFilterStat with (bits=${values.bits}, hashes=${values.hashes}) " +
                "into (bits=$bits, hashes=$hashes)"
        }
        // The hasher decides which bits a key sets, so OR-ing in words produced by a different mixer
        // relabels them as this filter's - and keys that were inserted then read back as absent,
        // which is the one thing a Bloom filter promises cannot happen.
        require(values.hasher == hasherRef) {
            "Cannot merge BloomFilterStat hashed with ${values.hasher} into one hashed with $hasherRef"
        }
        require(values.words.size == wordCount) {
            "Cannot merge BloomFilterStat: expected $wordCount words, got ${values.words.size}"
        }
        for (i in 0 until wordCount) {
            val incoming = values.words[i]
            if (incoming != 0L) casOr(words, i, incoming)
        }
        totalSeen.add(values.totalSeen)
    }

    override fun reset() {
        for (i in 0 until wordCount) words.store(i, 0L)
        totalSeen.store(0L)
    }

    override fun read(timestampNanos: Long): BloomFilterResult {
        val snapshot = LongArray(wordCount) { words.load(it) }
        return BloomFilterResult(
            bits = bits,
            hashes = hashes,
            words = snapshot,
            totalSeen = totalSeen.load(),
            hasher = hasherRef,
        )
    }

    override fun create(concurrency: Concurrency?) =
        BloomFilterStat(bits, hashes, hasher, concurrency ?: this.concurrency)
}
