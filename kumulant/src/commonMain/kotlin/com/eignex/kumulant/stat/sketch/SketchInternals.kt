package com.eignex.kumulant.stat.sketch

import com.eignex.kumulant.math.HasherRef
import kotlin.math.ceil

/**
 * Largest single increment a sketch counter accepts.
 *
 * Below `Long.MAX_VALUE` so a saturated counter stays distinguishable from an unvisited row and a further
 * update cannot wrap it negative, either of which breaks the one-sided guarantee.
 */
internal const val MAX_COUNTER_STEP: Long = Long.MAX_VALUE / 1024

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
