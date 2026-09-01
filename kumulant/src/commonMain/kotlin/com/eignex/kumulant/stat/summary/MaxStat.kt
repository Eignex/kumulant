package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.stream.casMax
import com.eignex.kumulant.stream.monotonicMode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Running maximum of a stream. */
@Serializable
@SerialName("MaxResult")
data class MaxResult(
    /** Running maximum value. */
    val max: Double,
) : Result

/**
 * Tracks the maximum value seen across a stream.
 *
 * **Use cases:** peak observation tracking, headroom checks, range
 * computation (paired with [MinStat] or directly via [RangeStat]).
 *
 * **Memory:** O(1); a single double cell.
 *
 * **Update:** O(1) per observation.
 *
 * **Concurrency:** Single-cell CAS-max loop; exact under every
 * [Concurrency] level. The CAS retry naturally serialises racing writers
 * without a lock.
 */
class MaxStat(override val concurrency: Concurrency = Concurrency.None) : SeriesStat<MaxResult> {

    private val mode = concurrency.monotonicMode()
    private val value = mode.newDouble(Double.NEGATIVE_INFINITY)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        casMax(this.value, value)
    }

    override fun merge(values: MaxResult, workspace: com.eignex.koblas.Workspace?) {
        casMax(value, values.max)
    }

    override fun reset() {
        value.store(Double.NEGATIVE_INFINITY)
    }

    override fun read(timestampNanos: Long) = MaxResult(value.load())

    override fun create(concurrency: Concurrency?) = MaxStat(concurrency ?: this.concurrency)
}
