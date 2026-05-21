package com.eignex.kumulant.stat.cardinality

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.stream.StreamLong
import com.eignex.kumulant.stream.StreamLongArray
import com.eignex.kumulant.stream.casOr
import com.eignex.kumulant.stream.monotonicMode
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
@SerialName("LinearCountingResult")
data class LinearCountingResult(
    /** Estimated cardinality `-bits * ln(unsetBits / bits)`. */
    val estimate: Double,
    /** Total bitset size in bits. */
    val bits: Int,
    /** Number of bits still cleared; saturates the estimator as it shrinks to zero. */
    val unsetBits: Long,
    /** Packed bitset (`bits / 64` longs); mergeable via word-wise OR. */
    val words: LongArray,
    /** Total observations the sketch has absorbed. */
    val totalSeen: Long,
) : Result

/**
 * Linear-counting cardinality estimator over a fixed [bits]-bit bitset.
 *
 * For each input, sets one bit indexed by `splitmix64(value) mod bits`. The cardinality
 * estimate is `-bits * ln(unsetBits / bits)`. Cheap and accurate for cardinalities up to
 * roughly [bits]; degrades sharply (and saturates to infinity) when the bitset fills.
 * Prefer [HyperLogLogStat] when the cardinality range is unknown.
 *
 * [bits] must be a power of two and a multiple of 64.
 *
 * **Use cases:** distinct-value estimation when cardinality is bounded and
 * the bitset can be sized comfortably above it. Cheaper and slightly more
 * accurate than [HyperLogLogStat] in that regime; saturates badly above it.
 *
 * **Memory:** O(bits / 64) Longs, plus two counters.
 *
 * **Update:** O(1) per observation; one hash + one atomic OR.
 *
 * **Concurrency:** Atomic OR on a striped Long array; counters are
 * independent atomic ops. Lock-free and exact under every [Concurrency]
 * level — bit sets are idempotent and commutative.
 */
class LinearCountingStat(val bits: Int = 4096, override val concurrency: Concurrency = Concurrency.None) :
    DiscreteStat<LinearCountingResult> {

    init {
        require(bits > 0) { "bits must be > 0" }
        require(bits and (bits - 1) == 0) { "bits must be a power of two" }
        require(bits % 64 == 0) { "bits must be a multiple of 64" }
    }

    private val wordCount: Int = bits / 64
    private val mask: Long = (bits - 1).toLong()
    private val mode = concurrency.monotonicMode()
    private val words: StreamLongArray = mode.newLongArray(wordCount)
    private val totalSeen: StreamLong = mode.newLong(0L)

    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0) return
        val hash = splitmix64(value)
        val pos = hash and mask
        val wordIdx = (pos ushr 6).toInt()
        val bitMask = 1L shl (pos and 63L).toInt()
        casOr(words, wordIdx, bitMask)
        totalSeen.add(1L)
    }

    override fun merge(values: LinearCountingResult) {
        require(values.bits == bits) {
            "Cannot merge LinearCountingStat with bits=${values.bits} into $bits"
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

    override fun read(timestampNanos: Long): LinearCountingResult {
        val snapshot = LongArray(wordCount)
        var setBits = 0L
        for (i in 0 until wordCount) {
            val w = words.load(i)
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

    override fun create(concurrency: Concurrency?) = LinearCountingStat(bits, concurrency ?: this.concurrency)
}
