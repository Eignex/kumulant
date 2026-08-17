package com.eignex.kumulant.core

/**
 * True when a weight carries no observation, and the update is therefore a no-op.
 *
 * Exactly zero, because every weighted recurrence in the library reduces to the identity at `w = 0`;
 * and any non-finite weight, because a weight is the multiplicity of an observation and neither `NaN`
 * nor an infinity is a multiplicity.
 *
 * The infinities are the less obvious half. `+Infinity` does have a clean mathematical reading - the
 * weighted mean update `w / (W + w)` tends to 1, so the mean tends to the observed value and the
 * variance to zero - but the recurrences do not reach that limit: they evaluate `Infinity / Infinity`
 * and `Infinity * 0` and yield `NaN`. Supporting the limit would mean special-casing every recurrence
 * up to the fourth moment, and would still leave the stat with `totalWeights = Infinity` permanently,
 * wedging the bias corrections that divide by it. A caller who wants to pin a stat to a value is better
 * served by saying so than by smuggling an infinity through the weight channel.
 *
 * Note that this is the *weight*, not the value: a `NaN` or infinite value is a real observation of an
 * unusable number and propagates. See [Stat] for the contract and why the two differ.
 */
// Inlined against the compiler's advice: it judges the impact by JVM standards, where the JIT would
// have inlined this anyway. The targets that matter here are the others - this call showed up once per
// update in the generated JS, across every stat that guards on it.
@Suppress("NOTHING_TO_INLINE")
internal inline fun Double.isInertWeight(): Boolean = !isFinite() || this == 0.0

/**
 * True when a weight is not a live positive observation, for the stats that cannot downdate.
 *
 * The sibling of [isInertWeight], and the difference between them is the whole point of having both
 * named. [isInertWeight] is for a stat whose recurrence inverts, so a negative weight is a legitimate
 * downdate and only zero and `NaN` are no-ops. This one is for a stat that has no inverse - a sketch,
 * a histogram bucket, an SGD step - where a negative weight is not a removal but a corruption, and is
 * dropped alongside the inert cases. See [Stat] for which stats fall on which side.
 *
 * `!(this > 0.0)` rather than `this <= 0.0 || isNaN()` because `NaN > 0.0` is already false: the `NaN`
 * case falls out of the comparison instead of needing a second clause a caller has to remember.
 *
 * The `isFinite` clause is not redundant with that: `+Infinity > 0.0` is true, so without it an infinite
 * weight would pass as an ordinary live observation and land in a histogram bucket or a sketch register
 * as infinite mass. See [isInertWeight] for why an infinity is not a multiplicity.
 */
@Suppress("NOTHING_TO_INLINE") // as with isInertWeight; the non-JVM targets pay for the call
internal inline fun Double.isNotPositiveWeight(): Boolean = !(this > 0.0) || !isFinite()

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
    // Callers drop a non-finite weight before reaching here, but keep the guard local too. `NaN > 0.0`
    // is false, so the require below would otherwise fire and report a NaN weight as a downdate emptying
    // the accumulator - wrong, since a NaN weight is a no-op. A `-Infinity` weight would trip it for the
    // same spurious reason.
    if (!weight.isFinite()) return
    require(currentTotal + weight > 0.0) {
        "weight $weight would take the accumulated weight from $currentTotal to " +
            "${currentTotal + weight}; a downdate must leave a positive total"
    }
}

/**
 * Interpret a `Double` as a class index, or reject it.
 *
 * A classifier takes its label through the same `Double` channel every other stat takes a value
 * through, so it has to decide which `Double`s name a class. [Stat] explains why a label is not a
 * value: a value is an observation and propagates, whereas a label is an *identifier*, and an
 * identifier that is not one of the identifiers cannot be folded in at all.
 *
 * Round-tripping through `Double` is what does the work, because `toInt()` alone is far too
 * permissive. It truncates toward zero, so `1.5` becomes class 1 and `-0.5` becomes class 0, and
 * `NaN.toInt()` is `0`, which means the most obviously invalid label in the language arrives looking
 * like a perfectly ordinary first class. Demanding `c.toDouble() == this` rejects all three: only a
 * `Double` that is exactly an integer survives.
 *
 * @param numClasses exclusive upper bound on the index; `numClasses` itself is not a class.
 * @return the class index, or `-1` if this `Double` does not name one. `-1` rather than `null` to keep
 *  the check allocation-free on the update path for the targets without escape analysis.
 */
@Suppress("NOTHING_TO_INLINE") // as with the weight predicates; the non-JVM targets pay for the call
internal inline fun Double.asClassLabel(numClasses: Int): Int {
    val c = toInt()
    return if (c.toDouble() == this && c >= 0 && c < numClasses) c else -1
}
