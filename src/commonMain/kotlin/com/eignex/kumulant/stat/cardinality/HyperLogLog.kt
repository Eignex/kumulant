package com.eignex.kumulant.stat.cardinality

import com.eignex.kumulant.core.DiscreteStat
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.stream.StreamLong
import com.eignex.kumulant.stream.StreamMode
import com.eignex.kumulant.stream.casMax
import com.eignex.kumulant.stream.defaultStreamMode
import com.eignex.kumulant.stream.splitmix64
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
@SerialName("HyperLogLog")
data class HyperLogLogResult(
    val estimate: Double,
    val precision: Int,
    val registers: IntArray,
    val totalSeen: Long,
) : Result

/**
 * HyperLogLog cardinality estimator with a small-range linear-counting fallback.
 *
 * Allocates `m = 2^precision` byte-sized registers and uses the standard
 * `α_m · m² / Σ 2^-Mj` estimator, switching to linear counting on small inputs
 * (`rawE ≤ 2.5·m` with at least one empty register) to eliminate the well-known
 * HLL bias near zero. Inputs are run through SplitMix64 before bucketing so
 * callers can pass raw IDs without worrying about hash quality.
 *
 * Memory: `m` Longs (registers) plus a counter. Standard error is ≈ `1.04/√m`
 * (≈ 0.81% at the default `precision = 14`). 64-bit hashing makes the original
 * HLL large-range correction unnecessary.
 *
 * This is plain HLL with the standard small-range linear-counting fix — *not*
 * HLL++. The Heule et al. (2013) empirical bias-correction tables and tighter
 * per-precision thresholds are not implemented (they work as a pair, and
 * porting half makes medium-range accuracy worse). Expect 3–5% downward bias
 * on cardinalities in the range `m … 5·m`; outside that window error stays
 * within the asymptotic `1.04/√m`. The sparse representation is also not
 * implemented — the linear-counting fallback already gives near-exact
 * estimates at low cardinalities.
 */
class HyperLogLog(
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

    override fun create(mode: StreamMode?) = HyperLogLog(precision, mode ?: this.mode)
}
