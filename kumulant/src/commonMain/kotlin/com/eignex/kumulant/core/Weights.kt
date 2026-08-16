package com.eignex.kumulant.core

/**
 * True when a weight carries no observation, and the update is therefore a no-op.
 *
 * Exactly zero, because every weighted recurrence in the library reduces to the identity at `w = 0`;
 * and `NaN`, because a weight is the multiplicity of an observation and `NaN` is not a multiplicity.
 * Note that this is the *weight*, not the value: a `NaN` value is a real observation of an unusable
 * number and propagates. See [Stat] for the contract and why the two differ.
 */
// Inlined against the compiler's advice: it judges the impact by JVM standards, where the JIT would
// have inlined this anyway. The targets that matter here are the others - this call showed up once per
// update in the generated JS, across every stat that guards on it.
@Suppress("NOTHING_TO_INLINE")
internal inline fun Double.isInertWeight(): Boolean = this == 0.0 || isNaN()

/**
 * True when a weight is not a live positive observation, for the stats that cannot downdate.
 *
 * The sibling of [isInertWeight], and the difference between them is the whole point of having both
 * named. [isInertWeight] is for a stat whose recurrence inverts, so a negative weight is a legitimate
 * downdate and only zero and `NaN` are no-ops. This one is for a stat that has no inverse - a sketch,
 * a histogram bucket, an SGD step - where a negative weight is not a removal but a corruption, and is
 * dropped alongside the inert cases. See [Stat] for which stats fall on which side.
 *
 * Written as a negated comparison rather than `this <= 0.0 || isNaN()` because `NaN > 0.0` is already
 * false: the `NaN` case falls out of the comparison instead of needing a second clause a caller has to
 * remember. Twenty call sites used to spell out the two-clause form, and the sites that spelled it out
 * slightly differently are exactly where the class-label defects lived.
 */
@Suppress("NOTHING_TO_INLINE") // as with isInertWeight; the non-JVM targets pay for the call
internal inline fun Double.isNotPositiveWeight(): Boolean = !(this > 0.0)

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
