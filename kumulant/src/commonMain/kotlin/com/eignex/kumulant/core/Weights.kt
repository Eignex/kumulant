package com.eignex.kumulant.core

// Keep these predicates inline on targets without JIT inlining; they run on every update.
// Non-finite weights cannot be used in the weighted recurrences without corrupting state.
@Suppress("NOTHING_TO_INLINE")
internal inline fun Double.isInertWeight(): Boolean = !isFinite() || this == 0.0

// Monotone sketches use this predicate to ignore downdates as well as inert weights.
@Suppress("NOTHING_TO_INLINE")
internal inline fun Double.isNotPositiveWeight(): Boolean = !(this > 0.0) || !isFinite()

// Welford recurrences divide by the updated total; a non-positive total corrupts later updates.
// Under Relaxed concurrency, validation and the following update can race.
internal fun requireLiveWeight(currentTotal: Double, weight: Double) {
    // Non-finite weights are no-ops, including when this guard is called directly.
    if (!weight.isFinite()) return
    require(currentTotal + weight > 0.0) {
        "weight $weight would take the accumulated weight from $currentTotal to " +
            "${currentTotal + weight}; a downdate must leave a positive total"
    }
}

// toInt() truncates fractions and maps NaN to zero, so an exact round-trip is required.
// The -1 sentinel avoids boxing a nullable result on the update path.
@Suppress("NOTHING_TO_INLINE")
internal inline fun Double.asClassLabel(numClasses: Int): Int {
    val c = toInt()
    return if (c.toDouble() == this && c >= 0 && c < numClasses) c else -1
}
