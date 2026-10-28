package com.eignex.kumulant.stat.sketch

import com.eignex.kumulant.math.HasherRef
import com.eignex.kumulant.stream.StreamLongArray
import kotlin.math.ceil

// Bounds individual increments to preserve the sketch counter guarantees.
internal const val MAX_COUNTER_STEP: Long = Long.MAX_VALUE / 1024

// Saturation preserves the one-sided count guarantee at upward overflow.
internal fun saturatingPlus(a: Long, b: Long): Long = if (b > 0L && a > Long.MAX_VALUE - b) Long.MAX_VALUE else a + b

// HighWrite array cells use the atomic backing, so CAS is available in every mode.
internal fun StreamLongArray.addSaturating(index: Int, delta: Long) {
    while (true) {
        val current = load(index)
        val next = saturatingPlus(current, delta)
        if (next == current || compareAndSet(index, current, next)) return
    }
}

// Rounding upward preserves the one-sided overestimate for fractional weights.
internal fun Double.toCounterStep(): Long = ceil(this).toLong().coerceIn(1L, MAX_COUNTER_STEP)

internal fun requireSameHasher(statName: String, incoming: HasherRef, own: HasherRef) {
    require(incoming == own) {
        "Cannot merge $statName hashed with $incoming into one hashed with $own"
    }
}
