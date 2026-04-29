package com.eignex.kumulant.stat.cardinality

import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.stream.StreamLong
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.casOr
import com.eignex.kumulant.stream.defaultStreamMode
import com.eignex.kumulant.stream.splitmix64
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.ln

/**
 * Linear-counting bitset snapshot. [estimate] = `-bits * ln(unsetBits / bits)`; saturates
 * to [Double.POSITIVE_INFINITY] when every bit is set. [words] is the raw bitset for
 * lossless merging.
 */
@Serializable
@SerialName("LinearCounting")
data class LinearCountingResult(
    val estimate: Double,
    val bits: Int,
    val unsetBits: Long,
    val words: LongArray,
    val totalSeen: Long,
) : Result

/**
 * Linear-counting cardinality estimator over a fixed [bits]-bit bitset.
 *
 * For each input, sets one bit indexed by `splitmix64(value) mod bits`. The cardinality
 * estimate is `-bits · ln(unsetBits / bits)`. Cheap and accurate for cardinalities up to
 * roughly [bits]; degrades sharply (and saturates to infinity) when the bitset fills.
 * Prefer [HyperLogLog] when the cardinality range is unknown.
 *
 * [bits] must be a power of two and a multiple of 64. Memory is `bits / 64` Longs.
 * Mergeable via word-wise OR.
 */
class LinearCounting(
    val bits: Int = 4096,
    val mode: StreamMode = defaultStreamMode,
) : DiscreteStat<LinearCountingResult> {

    init {
        require(bits > 0) { "bits must be > 0" }
        require(bits and (bits - 1) == 0) { "bits must be a power of two" }
        require(bits % 64 == 0) { "bits must be a multiple of 64" }
    }

    private val wordCount: Int = bits / 64
    private val mask: Long = (bits - 1).toLong()
    private val words: Array<StreamLong> = Array(wordCount) { mode.newLong(0L) }
    private val totalSeen: StreamLong = mode.newLong(0L)

    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0) return
        val hash = splitmix64(value)
        val pos = hash and mask
        val wordIdx = (pos ushr 6).toInt()
        val bitMask = 1L shl (pos and 63L).toInt()
        casOr(words[wordIdx], bitMask)
        totalSeen.add(1L)
    }

    override fun merge(values: LinearCountingResult) {
        require(values.bits == bits) {
            "Cannot merge LinearCounting with bits=${values.bits} into $bits"
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

    override fun read(timestampNanos: Long): LinearCountingResult {
        val snapshot = LongArray(wordCount)
        var setBits = 0L
        for (i in 0 until wordCount) {
            val w = words[i].load()
            snapshot[i] = w
            setBits += w.countOneBits()
        }
        val unset = bits - setBits
        val estimate = when {
            unset <= 0L -> Double.POSITIVE_INFINITY
            unset == bits.toLong() -> 0.0
            else -> -bits.toDouble() * ln(unset.toDouble() / bits.toDouble())
        }
        return LinearCountingResult(
            estimate = estimate,
            bits = bits,
            unsetBits = unset,
            words = snapshot,
            totalSeen = totalSeen.load(),
        )
    }

    override fun create(mode: StreamMode?) = LinearCounting(bits, mode ?: this.mode)
}
