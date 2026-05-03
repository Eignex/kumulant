package com.eignex.kumulant.stat.sketch

import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.stream.StreamLong
import com.eignex.kumulant.stream.StreamLongArray
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.casOr
import com.eignex.kumulant.stream.defaultStreamMode
import com.eignex.kumulant.stream.splitmix64
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

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
 * Bloom filter — probabilistic set-membership test with no false negatives. [bits] bits
 * are split across [hashes] positions per insert, derived from `splitmix64` via the
 * Kirsch–Mitzenmacher double-hashing scheme.
 *
 * False-positive rate is approximately `(1 - e^(-hashes * n / bits))^hashes` where `n` is
 * the number of distinct inserts. Memory is `bits / 64` Longs; mergeable element-wise via
 * bitwise OR when [bits] and [hashes] match.
 *
 * [bits] must be a power of two and a multiple of 64.
 */
class BloomFilter(
    val bits: Int = 1 shl 16,
    val hashes: Int = 7,
    override val mode: StreamMode = defaultStreamMode,
) : DiscreteStat<BloomFilterResult> {

    init {
        require(bits > 0) { "bits must be > 0" }
        require(bits and (bits - 1) == 0) { "bits must be a power of two" }
        require(bits % 64 == 0) { "bits must be a multiple of 64" }
        require(hashes > 0) { "hashes must be > 0" }
    }

    private val wordCount: Int = bits / 64
    private val mask: Long = (bits - 1).toLong()
    private val words: StreamLongArray = mode.newLongArray(wordCount)
    private val totalSeen: StreamLong = mode.newLong(0L)

    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0) return
        val h1 = splitmix64(value)
        val h2 = splitmix64(h1)
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
            "Cannot merge BloomFilter with (bits=${values.bits}, hashes=${values.hashes}) " +
                "into (bits=$bits, hashes=$hashes)"
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
        )
    }

    override fun create(mode: StreamMode?) = BloomFilter(bits, hashes, mode ?: this.mode)
}
