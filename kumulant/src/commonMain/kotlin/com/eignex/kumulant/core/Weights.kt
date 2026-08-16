package com.eignex.kumulant.core

/**
 * True when a weight carries no observation, and the update is therefore a no-op.
 *
 * Exactly zero, because every weighted recurrence in the library reduces to the identity at `w = 0`;
 * and `NaN`, because a weight is the multiplicity of an observation and `NaN` is not a multiplicity.
 * Note that this is the *weight*, not the value: a `NaN` value is a real observation of an unusable
 * number and propagates. See [Stat] for the contract and why the two differ.
 */
internal fun Double.isInertWeight(): Boolean = this == 0.0 || isNaN()

/**
 * Guard the one negative-weight case that corrupts a Welford accumulator.
 *
 * A negative [weight] is a legitimate downdate: it removes an observation that was
 * previously folded in, which is how a caller drives a sliding window by hand. The
 * recurrences invert exactly, so the removal is not an approximation.
 *
 * What is not recoverable is a downdate that takes the accumulated weight to zero or
 * below. Every Welford step divides by the new total, so a total of zero yields an
 * infinite or NaN mean that survives every later update, and a negative total silently
 * flips the sign of subsequent corrections. Both are corruption rather than a bad
 * answer, so this throws instead of letting the state rot.
 *
 * Under [Concurrency.Relaxed] the check is best-effort: the read of [currentTotal] and
 * the write that follows it are not atomic together, so a racing writer can slip
 * between them. That matches the level's documented contract, which already permits
 * drift. Every other level holds the stat's lock across both.
 *
 * @param currentTotal accumulated weight before this update.
 * @param weight the incoming observation weight; may be negative.
 * @throws IllegalArgumentException if the update would leave a non-positive total.
 */
internal fun requireLiveWeight(currentTotal: Double, weight: Double) {
    // Callers drop a NaN weight before reaching here, but keep the guard local too: `NaN > 0.0` is
    // false, so the require below would fire and report a NaN weight as a downdate emptying the
    // accumulator - wrong, and the one thing a NaN weight must not do now that it is a no-op.
    if (weight.isNaN()) return
    require(currentTotal + weight > 0.0) {
        "weight $weight would take the accumulated weight from $currentTotal to " +
            "${currentTotal + weight}; a downdate must leave a positive total"
    }
}
