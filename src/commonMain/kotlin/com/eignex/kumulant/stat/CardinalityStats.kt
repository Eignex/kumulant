package com.eignex.kumulant.stat

import com.eignex.kumulant.concurrent.StreamLong
import com.eignex.kumulant.concurrent.StreamMode
import com.eignex.kumulant.concurrent.defaultStreamMode
import com.eignex.kumulant.concurrent.splitmix64
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.HyperLogLogResult
import com.eignex.kumulant.core.LinearCountingResult
import kotlin.math.ln
import kotlin.math.pow

/**
 * HyperLogLog++ cardinality estimator with a small-range linear-counting fallback.
 *
 * Allocates `m = 2^precision` byte-sized registers and uses the standard
 * `α_m · m² / Σ 2^-Mj` estimator, switching to linear counting on small inputs to
 * eliminate the well-known HLL bias near zero. Inputs are run through SplitMix64
 * before bucketing so callers can pass raw IDs without worrying about hash quality.
 *
 * Memory: `m` Longs (registers) plus a counter. Standard error is ≈ `1.04/√m`
 * (≈ 0.81% at the default `precision = 14`). 64-bit hashing makes the original
 * HLL large-range correction unnecessary.
 *
 * The full HLL++ sparse representation is not implemented — at low cardinalities
 * the linear-counting fallback already gives near-exact estimates without the
 * memory savings.
 */
class HyperLogLogPlus(
    val precision: Int = 14,
    val mode: StreamMode = defaultStreamMode,
) : DiscreteStat<HyperLogLogResult> {

    init {
        require(precision in 4..18) { "precision must be in 4..18" }
    }

    private val m: Int = 1 shl precision
    private val alpha: Double = when (m) {
        16 -> 0.673
        32 -> 0.697
        64 -> 0.709
        else -> 0.7213 / (1.0 + 1.079 / m)
    }

    private val registers: Array<StreamLong> = Array(m) { mode.newLong(0L) }
    private val totalSeen: StreamLong = mode.newLong(0L)

    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        if (weight <= 0.0) return
        val hash = splitmix64(value)
        val idx = (hash ushr (64 - precision)).toInt() and (m - 1)
        val w = hash shl precision
        val rho = (w.countLeadingZeroBits().coerceAtMost(64 - precision)) + 1
        casMax(registers[idx], rho.toLong())
        totalSeen.add(1L)
    }

    private fun casMax(cell: StreamLong, candidate: Long) {
        while (true) {
            val current = cell.load()
            if (candidate <= current) return
            if (cell.compareAndSet(current, candidate)) return
        }
    }

    override fun merge(values: HyperLogLogResult) {
        require(values.precision == precision) {
            "Cannot merge HyperLogLog with precision ${values.precision} into ${precision}"
        }
        for (i in registers.indices) {
            val incoming = values.registers[i].toLong()
            if (incoming > 0L) casMax(registers[i], incoming)
        }
        totalSeen.add(values.totalSeen)
    }

    override fun reset() {
        for (cell in registers) cell.store(0L)
        totalSeen.store(0L)
    }

    override fun read(timestampNanos: Long): HyperLogLogResult {
        val snapshot = IntArray(m)
        var sumInv = 0.0
        var zeros = 0
        for (i in 0 until m) {
            val r = registers[i].load().toInt()
            snapshot[i] = r
            sumInv += 2.0.pow(-r)
            if (r == 0) zeros++
        }
        val rawE = alpha * m.toDouble() * m.toDouble() / sumInv
        val estimate = if (rawE <= 2.5 * m && zeros > 0) {
            m * ln(m.toDouble() / zeros)
        } else {
            rawE
        }
        return HyperLogLogResult(
            estimate = estimate,
            precision = precision,
            registers = snapshot,
            totalSeen = totalSeen.load(),
        )
    }

    override fun create(mode: StreamMode?) = HyperLogLogPlus(precision, mode ?: this.mode)
}

/**
 * Linear-counting cardinality estimator over a fixed [bits]-bit bitset.
 *
 * For each input, sets one bit indexed by `splitmix64(value) mod bits`. The cardinality
 * estimate is `-bits · ln(unsetBits / bits)`. Cheap and accurate for cardinalities up to
 * roughly [bits]; degrades sharply (and saturates to infinity) when the bitset fills.
 * Prefer [HyperLogLogPlus] when the cardinality range is unknown.
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

    private fun casOr(cell: StreamLong, bitMask: Long) {
        while (true) {
            val current = cell.load()
            val updated = current or bitMask
            if (current == updated) return
            if (cell.compareAndSet(current, updated)) return
        }
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
