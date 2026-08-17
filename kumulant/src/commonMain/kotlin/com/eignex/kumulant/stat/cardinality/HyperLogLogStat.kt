package com.eignex.kumulant.stat.cardinality

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.HasObservationCount
import com.eignex.kumulant.core.isNotPositiveWeight
import com.eignex.kumulant.core.preview
import com.eignex.kumulant.math.HasherRef
import com.eignex.kumulant.math.LongHasher
import com.eignex.kumulant.math.SplitMix64
import com.eignex.kumulant.stat.sketch.requireSameHasher
import com.eignex.kumulant.stream.StreamLong
import com.eignex.kumulant.stream.StreamLongArray
import com.eignex.kumulant.stream.casMax
import com.eignex.kumulant.stream.monotonicMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.ln
import kotlin.math.pow

/**
 * HyperLogLog snapshot. [estimate] is the corrected cardinality (linear counting at the
 * small-range, raw HLL elsewhere). [registers] are the dense `rho` values per bucket and
 * are required to merge two snapshots without loss; [precision] selects bucket count
 * `m = 2^precision`. [totalSeen] is the unweighted update count.
 */
@Serializable
@SerialName("HyperLogLogResult")
data class HyperLogLogResult(
    val estimate: Double,
    val precision: Int,
    val registers: IntArray,
    val totalSeen: Long,
    /**
     * Reference to the [com.eignex.kumulant.math.LongHasher] that produced this sketch.
     *
     * The mixer decides which register a key touches, so combining sketches built with different
     * mixers is meaningless - it double-counts the same key. Carried on the wire so [HyperLogLogStat.merge] can
     * refuse it, matching [com.eignex.kumulant.stat.sketch.BloomFilterResult] and
     * [com.eignex.kumulant.stat.sketch.CountMinSketchResult]. Defaults to the library default so a
     * payload encoded before the field existed still decodes.
     */
    val hasher: HasherRef = HasherRef.SplitMix64,
) : HasObservationCount {

    override val totalWeights: Double get() = totalSeen.toDouble()
    override fun equals(other: Any?): Boolean = other is HyperLogLogResult &&
        estimate.equals(other.estimate) &&
        precision == other.precision &&
        registers.contentEquals(other.registers) &&
        totalSeen == other.totalSeen &&
        hasher == other.hasher

    override fun hashCode(): Int {
        var h = estimate.hashCode()
        h = 31 * h + precision.hashCode()
        h = 31 * h + registers.contentHashCode()
        h = 31 * h + totalSeen.hashCode()
        h = 31 * h + hasher.hashCode()
        return h
    }

    override fun toString(): String = "HyperLogLogResult(" +
        "estimate=$estimate, " +
        "precision=$precision, " +
        "registers=${registers.preview()}, " +
        "totalSeen=$totalSeen)"
}

/**
 * HyperLogLog cardinality estimator with a small-range linear-counting fallback.
 *
 * Allocates `m = 2^precision` byte-sized registers and uses the standard
 * `alpha_m * m^2 / Sum 2^-Mj` estimator, switching to linear counting on small inputs
 * (`rawE <= 2.5*m` with at least one empty register) to eliminate the well-known
 * HLL bias near zero. Inputs are run through the [hasher] (default [SplitMix64]) before
 * bucketing so callers can pass raw IDs without worrying about hash quality.
 *
 * Memory: `m` Longs (registers) plus a counter. Standard error is ~ `1.04/sqrtm`
 * (~ 0.81% at the default `precision = 14`). 64-bit hashing makes the original
 * HLL large-range correction unnecessary.
 *
 * This is plain HLL with the standard small-range linear-counting fix; *not*
 * HLL++. The Heule et al. (2013) empirical bias-correction tables are not
 * implemented; empirically, with SplitMix64 prehashing the medium-range bias
 * stays inside `1.04/sqrtm` across `m ... 5*m` (see the accuracy test in
 * `HyperLogLogTest`). The sparse representation is also omitted; the
 * linear-counting fallback already gives near-exact estimates at low
 * cardinalities.
 *
 * **Use cases:** distinct-value estimation under tight memory (count unique
 * users, unique error fingerprints, unique IPs). Reach for [LinearCountingStat]
 * instead when cardinality is bounded and known to stay below the bitset size.
 *
 * **Memory:** O(m) = O(2^precision) bytes, plus a `totalSeen` counter.
 *
 * **Update:** O(1) per observation; one hash + register CAS-max.
 *
 * **Concurrency:** Per-register single-cell CAS-max loop on a striped Long
 * array; `totalSeen` is a separate atomic add. Lock-free and exact under
 * every [Concurrency] level; racing writers on the same register preserve
 * the max-over-incoming-rho invariant via CAS retry.
 */
class HyperLogLogStat(
    /** Number of register-index bits; memory is `2^precision` bytes. */
    val precision: Int = 14,
    /** Mixer applied to each input before bucketing; defaults to [SplitMix64]. */
    val hasher: LongHasher = SplitMix64,
    override val concurrency: Concurrency = Concurrency.None,
) : DiscreteStat<HyperLogLogResult> {

    init {
        require(precision in 4..18) { "precision must be in 4..18" }
    }

    private val hasherRef: HasherRef = HasherRef(hasher.name)
    private val m: Int = 1 shl precision
    private val alpha: Double = when (m) {
        16 -> 0.673
        32 -> 0.697
        64 -> 0.709
        else -> 0.7213 / (1.0 + 1.079 / m)
    }

    private val mode = concurrency.monotonicMode()
    private val registers: StreamLongArray = mode.newLongArray(m)
    private val totalSeen: StreamLong = mode.newLong(0L)

    override fun update(value: Long, timestampNanos: Long, weight: Double) {
        if (weight.isNotPositiveWeight()) return
        val hash = hasher.mix(value)
        val idx = (hash ushr (64 - precision)).toInt() and (m - 1)
        val w = hash shl precision
        val rho = (w.countLeadingZeroBits().coerceAtMost(64 - precision)) + 1
        casMax(registers, idx, rho.toLong())
        totalSeen.add(1L)
    }

    override fun merge(values: HyperLogLogResult) {
        requireSameHasher("HyperLogLogStat", values.hasher, hasherRef)
        // Shape before array length: `m` is derived from `precision`, so checking the register count
        // first would report a differently-sized sketch as the wrong register count rather than the
        // wrong precision, which is what the caller actually got wrong.
        require(values.precision == precision) {
            "Cannot merge HyperLogLogStat with precision ${values.precision} into $precision"
        }
        require(values.registers.size == m) {
            "Cannot merge HyperLogLogStat: expected $m registers, got ${values.registers.size}"
        }
        for (i in 0 until m) {
            val incoming = values.registers[i].toLong()
            if (incoming > 0L) casMax(registers, i, incoming)
        }
        totalSeen.add(values.totalSeen)
    }

    override fun reset() {
        for (i in 0 until m) registers.store(i, 0L)
        totalSeen.store(0L)
    }

    override fun read(timestampNanos: Long): HyperLogLogResult {
        val snapshot = IntArray(m)
        var sumInv = 0.0
        var zeros = 0
        for (i in 0 until m) {
            val r = registers.load(i).toInt()
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
            hasher = hasherRef,
        )
    }

    override fun create(concurrency: Concurrency?) = HyperLogLogStat(precision, hasher, concurrency ?: this.concurrency)
}
