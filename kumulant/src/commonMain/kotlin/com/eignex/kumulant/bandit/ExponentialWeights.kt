package com.eignex.kumulant.bandit

import com.eignex.kumulant.math.argMaxOf
import kotlin.random.Random

/**
 * Floor applied to the played probability before it divides an importance-weighted reward.
 *
 * EXP3 and EXP4 both estimate an unbiased gain as `reward / p(played arm)`, so an arm the policy had
 * all but written off produces an enormous gain estimate, and one it had written off entirely produces
 * a non-finite one. The floor bounds that blow-up. It is deliberately far below any probability the
 * exploration term `gamma / K` can produce, so it never perturbs a live distribution.
 */
internal const val MIN_PLAY_PROB: Double = 1e-12

/** Above this maximum weight, rescale before `exp` overflows to infinity. */
internal const val RENORM_THRESHOLD: Double = 1e100

/** Below this maximum weight, rescale before the whole array underflows to zero. */
internal const val RENORM_FLOOR: Double = 1e-100

/**
 * Keep an array of exponential weights inside the range where `exp` still carries information.
 *
 * The distribution these weights induce depends only on their *ratios*, so dividing the array through
 * by its maximum is free: it changes no arm's or expert's relative standing. That is what makes both
 * ends of the range recoverable rather than just the top one.
 *
 * Both ends need guarding. A run of large negative rewards drives every `exp(eta * gain)` toward zero,
 * and once the array is *entirely* zero no later reward can lift it: every subsequent update multiplies
 * zero by something. Rescaling by the surviving maximum keeps the array alive; the fully collapsed case
 * has no ratios left to preserve, so it resets to uniform.
 */
internal fun DoubleArray.renormaliseExponentialWeights() {
    var maxW = 0.0
    for (w in this) if (w > maxW) maxW = w
    when {
        maxW > RENORM_THRESHOLD || (maxW > 0.0 && maxW < RENORM_FLOOR) ->
            for (i in indices) this[i] /= maxW

        maxW == 0.0 -> for (i in indices) this[i] = 1.0
    }
}

/**
 * Draw an index from a normalised probability array by inverse CDF.
 *
 * Falls through to the last index when the probabilities sum to slightly under one, which floating-point
 * normalisation routinely produces. An entry of exactly `0.0` never wins.
 */
internal fun Random.sampleFromDistribution(p: DoubleArray): Int {
    var u = nextDouble()
    for (a in p.indices) {
        u -= p[a]
        if (u < 0.0) return a
    }
    return p.size - 1
}

/** Index of the highest-scoring arm; see [argMaxOf] for the tie and NaN rules. */
internal inline fun argmaxArm(nbrArms: Int, score: (Int) -> Double): Int = argMaxOf(nbrArms, score)
