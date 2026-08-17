package com.eignex.kumulant.stat.sketch

import com.eignex.kumulant.math.HasherRef
import kotlin.math.ceil

/**
 * Largest single increment a sketch counter accepts.
 *
 * Below `Long.MAX_VALUE` on purpose: saturating a counter at the type's ceiling used to make it
 * indistinguishable from an unvisited row, and a second such update wrapped it negative - both of which
 * broke the one-sided guarantee the sketches exist to provide.
 *
 * [CountMinSketchStat] and [SpaceSavingStat] each declared this, under different names, and the second
 * one's KDoc said only that it "mirrors `CountMinSketchStat.MAX_COUNTER_STEP`". A comment was the entire
 * mechanism keeping two counter ceilings equal.
 */
internal const val MAX_COUNTER_STEP: Long = Long.MAX_VALUE / 1024

/**
 * Round a fractional observation weight up to the integer counter step a sketch can hold.
 *
 * Counting sketches store integers, so a weight has to be rounded before it can be added; rounding *up*
 * keeps the one-sided overestimate the guarantees are stated in terms of. The clamp at both ends is what
 * stops a zero or a huge weight breaking those guarantees, and both stats spelled the whole expression
 * out identically.
 */
internal fun Double.toCounterStep(): Long = ceil(this).toLong().coerceIn(1L, MAX_COUNTER_STEP)

/**
 * Refuse to merge two sketches built on different hash functions.
 *
 * All five hashing sketches carried this check with the same message shape and its own copy of the
 * rationale. The rationale is worth stating once: a sketch's registers, bits or signatures are only
 * comparable under the same hash, so combining across hashers does not degrade the estimate gracefully -
 * it produces a number with no error bound at all, which is strictly worse than refusing.
 *
 * @param statName the stat's own name, which is all that varied between the five copies.
 * @param incoming the hasher recorded on the snapshot being merged in.
 * @param own the hasher this stat was built with.
 */
internal fun requireSameHasher(statName: String, incoming: HasherRef, own: HasherRef) {
    require(incoming == own) {
        "Cannot merge $statName hashed with $incoming into one hashed with $own"
    }
}
