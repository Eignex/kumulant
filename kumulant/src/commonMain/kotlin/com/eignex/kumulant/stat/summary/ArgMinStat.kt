package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.monotonicMode
import com.eignex.kumulant.stream.serializedLock
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

    private val mode = concurrency.monotonicMode()
    private val lock = concurrency.serializedLock()
    private val value = mode.newDouble(Double.POSITIVE_INFINITY)
    private val at = mode.newLong(0L)

    override fun update(value: Double, timestampNanos: Long, weight: Double) = lock.guarded {
        if (weight == 0.0 || value.isNaN()) return@guarded // zero weight and NaN are both no-ops; see Stat
        if (value < this.value.load()) {
            this.value.store(value)
            at.store(timestampNanos)
        }
    }

    override fun merge(values: ArgMinResult) = lock.guarded {
        if (values.min < value.load()) {
            value.store(values.min)
            at.store(values.atTimestampNanos)
        }
    }

    override fun reset() = lock.guarded {
        value.store(Double.POSITIVE_INFINITY)
        at.store(0L)
    }

    override fun read(timestampNanos: Long) = lock.guarded {
        ArgMinResult(value.load(), at.load())
    }

    override fun create(concurrency: Concurrency?) = ArgMinStat(concurrency ?: this.concurrency)
}
