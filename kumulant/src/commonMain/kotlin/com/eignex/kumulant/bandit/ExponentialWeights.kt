package com.eignex.kumulant.bandit

import com.eignex.kumulant.math.argMaxOf
import kotlin.random.Random

// The probability floor bounds reward / p during importance weighting.
internal const val MIN_PLAY_PROB: Double = 1e-12

// Rescale before exp overflows.
internal const val RENORM_THRESHOLD: Double = 1e100

// Rescale before all weights underflow to zero.
internal const val RENORM_FLOOR: Double = 1e-100

// Scaling by the maximum preserves probability ratios; fully collapsed weights reset to uniform.
internal fun DoubleArray.renormaliseExponentialWeights() {
    var maxW = 0.0
    for (w in this) if (w > maxW) maxW = w
    when {
        maxW > RENORM_THRESHOLD || (maxW > 0.0 && maxW < RENORM_FLOOR) ->
            for (i in indices) this[i] /= maxW

        maxW == 0.0 -> for (i in indices) this[i] = 1.0
    }
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
