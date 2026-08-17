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
 * @return `true` if the array now holds probabilities. The `false` case is carried over from the three
 *  copies this replaces, all of which guarded on the sum being positive, and it is retained so this
 *  behaves exactly as they did - but it cannot currently fire. The shift guarantees one element is
 *  `exp(0) == 1`, so a finite input always sums to at least 1; and a non-finite input makes the sum
 *  `NaN`, which fails `<= 0.0` too. A `NaN` logit therefore returns `true` with a `NaN` array, which is
 *  how a single `NaN` feature poisons a softmax model's weights for good. That is a real defect, older
 *  than this function and unchanged by it, and fixing it means deciding what a model should do with an
 *  unusable feature rather than tightening this guard alone.
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
    // observation on the training path, and the update site already spelled it this way.
    val invZ = 1.0 / z
    for (i in indices) this[i] *= invZ
    return true
}

/**
 * Floor and ceiling applied to a probability before taking its logarithm.
 *
 * `ln(0)` is `-Infinity`, which poisons a running mean permanently, so every log-loss-shaped score has to
 * clamp first. The value was declared twice under two names - `LOG_LOSS_EPS` in the score family and
 * `SOFTMAX_EPS` in the softmax regressor - for the same job, plus a third time in prose in a KDoc.
 *
 * `1e-15` rather than something smaller: `ln(1e-15)` is about -34.5, a large but finite penalty, while a
 * clamp near the `2.2e-16` double epsilon would lose the distinction between the clamp and `1.0` at the
 * upper end, where the interesting quantity is `1 - p`.
 */
internal const val PROBABILITY_FLOOR: Double = 1e-15
