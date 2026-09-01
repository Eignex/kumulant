package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.Result
import com.eignex.kumulant.core.SeriesStat
import com.eignex.kumulant.core.isInertWeight
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.monotonicMode
import com.eignex.kumulant.stream.serializedLock
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

    private val mode = concurrency.monotonicMode()
    private val lock = concurrency.serializedLock()
    private val value = mode.newDouble(Double.NEGATIVE_INFINITY)
    private val at = mode.newLong(0L)

    override fun update(value: Double, timestampNanos: Long, weight: Double) {
        if (weight.isInertWeight()) return
        lock.guarded {
            if (value > this.value.load()) {
                this.value.store(value)
                at.store(timestampNanos)
            }
        }
    }

    override fun merge(values: ArgMaxResult, workspace: com.eignex.koblas.Workspace?) = lock.guarded {
        if (values.max > value.load()) {
            value.store(values.max)
            at.store(values.atTimestampNanos)
        }
    }

    override fun reset() = lock.guarded {
        value.store(Double.NEGATIVE_INFINITY)
        at.store(0L)
    }

    override fun read(timestampNanos: Long) = lock.guarded {
        ArgMaxResult(value.load(), at.load())
    }

    override fun create(concurrency: Concurrency?) = ArgMaxStat(concurrency ?: this.concurrency)
}
