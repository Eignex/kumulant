package com.eignex.kumulant.math

import kotlin.math.exp

// Max subtraction prevents exponent overflow for finite logits. NaN propagates through normalization,
// and the return value is true even when the normalized array contains NaN.
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
    val invZ = 1.0 / z
    for (i in indices) this[i] *= invZ
    return true
}

// Keeps log loss finite and 1 - p distinguishable from zero at the upper bound.
internal const val PROBABILITY_FLOOR: Double = 1e-15
