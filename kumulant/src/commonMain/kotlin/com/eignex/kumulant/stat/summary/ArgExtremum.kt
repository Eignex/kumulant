package com.eignex.kumulant.stat.summary

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.monotonicMode
import com.eignex.kumulant.stream.serializedLock

// The (value, timestamp) pair cannot be made elementwise atomic with a single-cell CAS, so a lock
// keeps the pair consistent under every concurrency level. Ties keep the first occurrence.
internal class ArgExtremum(
    concurrency: Concurrency,
    private val identity: Double,
    private val improves: (candidate: Double, current: Double) -> Boolean,
) {
    private val mode = concurrency.monotonicMode()
    private val lock = concurrency.serializedLock()
    private val value = mode.newDouble(identity)
    private val at = mode.newLong(0L)

    fun offer(candidate: Double, timestampNanos: Long) = lock.guarded {
        if (improves(candidate, value.load())) {
            value.store(candidate)
            at.store(timestampNanos)
        }
    }

    fun reset() = lock.guarded {
        value.store(identity)
        at.store(0L)
    }

    fun <R> read(result: (Double, Long) -> R): R = lock.guarded { result(value.load(), at.load()) }
}
