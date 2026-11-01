package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isNotPositiveWeight
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Running minimum of a stream plus the timestamp at which it occurred. */
@Serializable
@SerialName("ArgMinResult")
data class ArgMinResult(
    /** Running minimum value. */
    val min: Double,
    /** Timestamp (nanoseconds) at which [min] was observed. */
    val atTimestampNanos: Long,
) : Result

/**
 * Tracks the minimum value seen across a stream together with the timestamp
 * at which it occurred. On ties the first occurrence wins.
 *
 * **Use cases:** best objective and when it was found, lowest latency and
 * when it happened, locating the trough of a series in time.
 *
 * **Memory:** O(1); a double cell, a long cell, and a lock.
 *
 * **Update:** O(1) per observation.
 *
 * **Concurrency:** Internal CAS spin-lock (category 3). The (value, timestamp)
 * pair cannot be made elementwise atomic with a single-cell CAS; the lock
 * keeps the pair consistent under any [Concurrency] level.
 */
class ArgMinStat(override val concurrency: Concurrency = Concurrency.None) : SeriesStat<ArgMinResult> {

    private val extremum = ArgExtremum(
        concurrency,
        Double.POSITIVE_INFINITY,
    ) { candidate, current -> candidate < current }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        // Non-positive rather than merely inert: an extremum or event trace has no inverse, so a
        // downdate would run forwards as an ordinary observation. See Stat.
        if (weight.isNotPositiveWeight()) return
        extremum.offer(value, timestampNanos)
    }

    override fun merge(values: ArgMinResult, timestampNanos: Long) = extremum.offer(values.min, values.atTimestampNanos)

    override fun reset() = extremum.reset()

    override fun read(timestampNanos: Long) = extremum.read(::ArgMinResult)

    override fun create(concurrency: Concurrency?) = ArgMinStat(concurrency ?: this.concurrency)
}
