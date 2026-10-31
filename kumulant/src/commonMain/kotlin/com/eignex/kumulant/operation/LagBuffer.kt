package com.eignex.kumulant.operation

import com.eignex.kumulant.core.Concurrency
import com.eignex.kumulant.core.isNotPositiveWeight
import com.eignex.kumulant.stream.guarded
import com.eignex.kumulant.stream.monotonicMode
import com.eignex.kumulant.stream.serializedLock

internal class LagBuffer(private val k: Int, concurrency: Concurrency) {
    private val mode = concurrency.monotonicMode()
    private val lock = concurrency.serializedLock()
    private val tick = mode.newLong(0L)
    private val ring = mode.newDoubleArray(k)

    // The ring exchange and forwarding share a lock so writers cannot observe an unfilled slot
    // or forward a pending observation after a reset. Inert weights must not enter future history.
    inline fun update(value: Double, weight: Double, emit: (Double) -> Unit) {
        // Non-positive rather than merely inert: this order-dependent recurrence has no inverse, so
        // a downdate would run forwards as an ordinary observation. See Stat.
        if (weight.isNotPositiveWeight()) return
        lock.guarded {
            val n = tick.addAndGet(1L)
            val slot = ((n - 1L) % k).toInt()
            val past = ring.load(slot)
            ring.store(slot, value)
            if (n > k) emit(past)
        }
    }

    inline fun reset(resetDelegate: () -> Unit) = lock.guarded {
        resetDelegate()
        tick.store(0L)
        for (i in 0 until k) ring.store(i, 0.0)
    }
}
