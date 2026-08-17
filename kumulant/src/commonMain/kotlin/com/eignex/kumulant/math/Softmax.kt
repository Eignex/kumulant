package com.eignex.kumulant.math

import kotlin.math.exp

/**
 * Turn an array of logits into probabilities in place, and report whether the sum was usable.
 *
 * Standard max-subtracting softmax: `exp(z - max)` before summing, so the largest exponent is exactly
 * `exp(0) == 1` and nothing overflows however large the logits get. Subtracting a constant from every
 * logit leaves the softmax unchanged, which is what makes the trick free.
 *
 * In place because all three call sites already own a scratch array they built for this, so returning a
 * fresh one would add an allocation per observation on the softmax training path.
 *
 * @return `true` if the array now holds probabilities. The non-empty `false` case cannot fire: the shift
 *  guarantees one element is `exp(0) == 1`, so a finite input always sums to at least 1, and a non-finite
 *  input makes the sum `NaN`, which fails `<= 0.0` too. A `NaN` logit therefore returns `true` with a
 *  `NaN` array, which is how a single `NaN` feature poisons a softmax model's weights for good. Fixing
 *  that means deciding what a model should do with an unusable feature, not tightening this guard.
 */
internal fun DoubleArray.softmaxInPlace(): Boolean {
    if (isEmpty()) return false
    var maxLogit = this[0]
    for (i in 1 until size) if (this[i] > maxLogit) maxLogit = this[i]
    var z = 0.0
    for (i in indices) {
        this[i] = exp(this[i] - maxLogit)
        z += this[i]
    }
    if (z <= 0.0) return false
    // One reciprocal and a multiply per element rather than a divide per element; this runs once per
    // observation on the training path.
    val invZ = 1.0 / z
    for (i in indices) this[i] *= invZ
    return true
}

/**
 * Floor and ceiling applied to a probability before taking its logarithm.
 *
 * `ln(0)` is `-Infinity`, which poisons a running mean permanently, so every log-loss-shaped score clamps
 * first. `1e-15` gives a large but finite penalty at about -34.5, while staying far enough from the
 * `2.2e-16` double epsilon that `1 - p` is still distinguishable from zero at the upper end.
 */
internal const val PROBABILITY_FLOOR: Double = 1e-15
