package com.eignex.kumulant.stat.sketch

import com.eignex.kumulant.math.HasherRef
import com.eignex.kumulant.stream.StreamLongArray
import kotlin.math.ceil

/**
 * Largest single increment a sketch counter accepts.
 *
 * Below `Long.MAX_VALUE` so a saturated counter stays distinguishable from an unvisited row and a further
 * update cannot wrap it negative, either of which breaks the one-sided guarantee.
 */
internal const val MAX_COUNTER_STEP: Long = Long.MAX_VALUE / 1024

/**
 * Sum that pins at [Long.MAX_VALUE] instead of wrapping.
 *
 * [MAX_COUNTER_STEP] bounds one increment, never the running total, so a long enough stream overflows a
 * counter whatever the step was. A wrapped counter reads *below* the true count, which is the one thing
 * a sketch built on a one-sided overestimate must never do; pinned at the ceiling it stays useless but
 * sound. Only upward overflow is guarded, since a sketch never subtracts its way past the floor.
 */
internal fun saturatingPlus(a: Long, b: Long): Long = if (b > 0L && a > Long.MAX_VALUE - b) Long.MAX_VALUE else a + b

/**
 * Add [delta] at [index], pinning at [Long.MAX_VALUE] rather than wrapping.
 *
 * A read-modify-write rather than a blind add, which a striped adder would not support - but the
 * HighWrite mode hands array cells to the atomic backing rather than striping them, so the
 * compare-and-set is there on every concurrency level.
 */
internal fun StreamLongArray.addSaturating(index: Int, delta: Long) {
    while (true) {
        val current = load(index)
        val next = saturatingPlus(current, delta)
        if (next == current || compareAndSet(index, current, next)) return
    }
}

/**
 * Round a fractional observation weight up to the integer counter step a sketch can hold.
 *
 * Rounding *up* preserves the one-sided overestimate the guarantees are stated in terms of; the clamp
 * keeps a zero or an enormous weight from breaking them.
 */
internal fun Double.toCounterStep(): Long = ceil(this).toLong().coerceIn(1L, MAX_COUNTER_STEP)

/**
 * Refuse to merge two sketches built on different hash functions.
 *
 * Registers, bits and signatures are only comparable under the same hash, so combining across hashers
 * does not degrade the estimate gracefully - it yields a number with no error bound at all.
 *
 * @param statName the stat's own name, for the message.
 * @param incoming the hasher recorded on the snapshot being merged in.
 * @param own the hasher this stat was built with.
 */
internal fun requireSameHasher(statName: String, incoming: HasherRef, own: HasherRef) {
    require(incoming == own) {
        "Cannot merge $statName hashed with $incoming into one hashed with $own"
    }
}
