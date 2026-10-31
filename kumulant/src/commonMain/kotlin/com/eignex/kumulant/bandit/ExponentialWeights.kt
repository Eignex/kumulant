package com.eignex.kumulant.bandit

import com.eignex.kumulant.math.argMaxOf
import kotlin.random.Random

// The probability floor bounds reward / p during importance weighting.
internal const val MIN_PLAY_PROB: Double = 1e-12

// Exponential weights are held as logs: exp(eta * gain) overflows for eta * gain above ~709, and a
// weight underflowed to zero can never recover. Clamping keeps every log weight finite.
internal fun Double.clampLogWeight(): Double = coerceIn(-Double.MAX_VALUE, Double.MAX_VALUE)

// Shifting by the maximum preserves probability ratios and keeps the largest exp(logWeight) at one.
internal fun DoubleArray.normaliseLogWeights() {
    var maxLog = Double.NEGATIVE_INFINITY
    for (w in this) if (w > maxLog) maxLog = w
    if (!maxLog.isFinite()) return
    for (i in indices) this[i] = (this[i] - maxLog).clampLogWeight()
}

// Floating-point normalization can leave a remainder, assigned to the last index.
internal fun Random.sampleFromDistribution(p: DoubleArray): Int {
    var u = nextDouble()
    for (a in p.indices) {
        u -= p[a]
        if (u < 0.0) return a
    }
    return p.size - 1
}

internal inline fun argmaxArm(nbrArms: Int, score: (Int) -> Double): Int = argMaxOf(nbrArms, score)
