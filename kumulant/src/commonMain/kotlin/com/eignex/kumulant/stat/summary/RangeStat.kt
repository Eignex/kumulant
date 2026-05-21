package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.casMax
import com.eignex.kumulant.stream.casMin
import com.eignex.kumulant.stream.monotonicMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Running min/max pair of a stream. */
@Serializable
@SerialName("RangeResult")
data class RangeResult(
    /** Running minimum value. */
    val min: Double,
    /** Running maximum value. */
    val max: Double,
) : Result

/**
 * Tracks the minimum and maximum value seen across a stream.
 *
 * **Use cases:** observed range / spread monitoring; pairs with quantile stats
 * to confirm coverage of the range under observation.
 *
 * **Memory:** O(1) — two double cells.
 *
 * **Update:** O(1) per observation (one CAS-min + one CAS-max).
 *
 * **Concurrency:** Two independent CAS-min/CAS-max cells — each exact under
 * every [Concurrency] level. A `read()` between the two CAS writes of a
 * single update can briefly observe `min > max` on a never-yet-updated stat
 * under heavy contention, but the per-cell guarantees hold.
 */
class RangeStat(override val concurrency: Concurrency = Concurrency.None) : SeriesStat<RangeResult> {

    private val mode = concurrency.monotonicMode()
    private val min = mode.newDouble(Double.POSITIVE_INFINITY)
    private val max = mode.newDouble(Double.NEGATIVE_INFINITY)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        casMin(min, value)
        casMax(max, value)
    }

    override fun merge(values: RangeResult) {
        casMin(min, values.min)
        casMax(max, values.max)
    }

    override fun reset() {
        min.store(Double.POSITIVE_INFINITY)
        max.store(Double.NEGATIVE_INFINITY)
    }

    override fun read(timestampNanos: Long) = RangeResult(min.load(), max.load())

    override fun create(concurrency: Concurrency?) = RangeStat(concurrency ?: this.concurrency)
}
