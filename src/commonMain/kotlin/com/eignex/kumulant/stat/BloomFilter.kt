package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.StreamLong
import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.concurrent.splitmix64
import com.eignex.kumulant.core.BloomFilterResult
import com.eignex.kumulant.core.DiscreteStat

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
    val mode: StreamMode = defaultStreamMode,
) : DiscreteStat<BloomFilterResult> {

    init {
        require(bits > 0) { "bits must be > 0" }
        require(bits and (bits - 1) == 0) { "bits must be a power of two" }
        require(bits % 64 == 0) { "bits must be a multiple of 64" }
        require(hashes > 0) { "hashes must be > 0" }
    }

    private val wordCount: Int = bits / 64
    private val mask: Long = (bits - 1).toLong()
    private val words: Array<StreamLong> = Array(wordCount) { mode.newLong(0L) }
    private val totalSeen: StreamLong = mode.newLong(0L)

    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0) return
        val h1 = splitmix64(value)
        val h2 = splitmix64(h1)
        for (i in 0 until hashes) {
            val pos = (h1 + i.toLong() * h2) and mask
            val wordIdx = (pos ushr 6).toInt()
            val bitMask = 1L shl (pos and 63L).toInt()
            casOr(words[wordIdx], bitMask)
        }
        totalSeen.add(1L)
    }

    private fun casOr(cell: StreamLong, bitMask: Long) {
        while (true) {
            val current = cell.load()
            val updated = current or bitMask
            if (current == updated) return
            if (cell.compareAndSet(current, updated)) return
        }
    }

    override fun merge(values: BloomFilterResult) {
        require(values.bits == bits && values.hashes == hashes) {
            "Cannot merge BloomFilter with (bits=${values.bits}, hashes=${values.hashes}) into (bits=$bits, hashes=$hashes)"
        }
        for (i in words.indices) {
            val incoming = values.words[i]
            if (incoming != 0L) casOr(words[i], incoming)
        }
        totalSeen.add(values.totalSeen)
    }

    override fun reset() {
        for (cell in words) cell.store(0L)
        totalSeen.store(0L)
    }

    override fun read(timestampNanos: Long): BloomFilterResult {
        val snapshot = LongArray(wordCount) { words[it].load() }
        return BloomFilterResult(
            bits = bits,
            hashes = hashes,
            words = snapshot,
            totalSeen = totalSeen.load(),
        )
    }

    override fun create(mode: StreamMode?) = BloomFilter(bits, hashes, mode ?: this.mode)
}
