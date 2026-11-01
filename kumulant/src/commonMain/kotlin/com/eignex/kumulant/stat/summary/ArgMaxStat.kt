package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isNotPositiveWeight
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Running maximum of a stream plus the timestamp at which it occurred. */
@Serializable
@SerialName("ArgMaxResult")
data class ArgMaxResult(
    /** Running maximum value. */
    val max: Double,
    /** Timestamp (nanoseconds) at which [max] was observed. */
    val atTimestampNanos: Long,
) : Result

/**
 * Tracks the maximum value seen across a stream together with the timestamp
 * at which it occurred. On ties the first occurrence wins.
 *
 * **Use cases:** best objective and when it was found, peak load and when
 * it happened, locating the peak of a series in time.
 *
 * **Memory:** O(1); a double cell, a long cell, and a lock.
 *
 * **Update:** O(1) per observation.
 *
 * **Concurrency:** Internal CAS spin-lock (category 3). The (value, timestamp)
 * pair cannot be made elementwise atomic with a single-cell CAS; the lock
 * keeps the pair consistent under any [Concurrency] level.
 */
class ArgMaxStat(override val concurrency: Concurrency = Concurrency.None) : SeriesStat<ArgMaxResult> {

    private val extremum = ArgExtremum(
        concurrency,
        Double.NEGATIVE_INFINITY,
    ) { candidate, current -> candidate > current }

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        // Non-positive rather than merely inert: an extremum or event trace has no inverse, so a
        // downdate would run forwards as an ordinary observation. See Stat.
        if (weight.isNotPositiveWeight()) return
        extremum.offer(value, timestampNanos)
    }

    override fun merge(values: ArgMaxResult, timestampNanos: Long) = extremum.offer(values.max, values.atTimestampNanos)

    override fun reset() = extremum.reset()

    override fun read(timestampNanos: Long) = extremum.read(::ArgMaxResult)

    override fun create(concurrency: Concurrency?) = ArgMaxStat(concurrency ?: this.concurrency)
}
