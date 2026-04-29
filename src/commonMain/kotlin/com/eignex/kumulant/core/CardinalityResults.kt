package com.eignex.kumulant.core

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

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
